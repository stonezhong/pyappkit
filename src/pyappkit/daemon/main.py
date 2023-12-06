import importlib
import sys
import signal
import os
import logging
import logging.config
import time
from enum import Enum
from typing import Tuple, Any, Optional, IO, Dict, Callable, List
import logging.config
import argparse
import json
from datetime import timedelta
import math

from multiprocessing import Process, Value

##################################################################################################
# If guardian receives SIGTERM, it kills executor process if there is one
##################################################################################################

class DaemonRunStatus(Enum):
    LAUNCHED = 1
    ALREADY_RUNNING = 2
    REDIR_STDOUT_FAILED = 3
    REDIR_STDERR_FAILED = 4
    FORK_FAILED = 5

class ExecutorStatus(Enum):
    FORK_FAILED = 1
    FINISHED = 2

__APP_CONTEXT = {
    'quit_requested': False,
    'executor_pid': None,
    'is_guardian': True
}

def __request_guardian_shutdown(signal_umber, frame):
    _, _ = signal_umber, frame
    __APP_CONTEXT['quit_requested'] = True
    executor_pid = __APP_CONTEXT['executor_pid']
    if executor_pid is not None:
        __safe_kill(executor_pid)

def __request_executor_shutdown(signal_umber, frame):
    _, _ = signal_umber, frame
    __APP_CONTEXT['quit_requested'] = True


def __quit_requested():
    return __APP_CONTEXT['quit_requested']


def __get_method(method_name):
    module_name, entry_name = method_name.split(":")
    module = importlib.import_module(module_name)
    entry = getattr(module, entry_name)
    return entry


def sleep_if(count, condition, step=1):
    for _ in range(0, count):
        if not condition():
            return
        time.sleep(step)


def __read_int_file(filename: str) -> Optional[int]:
    if not os.path.isfile(filename):
        return None
    with open(filename, "rt") as f:
        return int(f.read())


def __safe_remove(filename: str):
    try:
        os.remove(filename)
    except OSError:
        pass

def __safe_kill(pid:int):
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        pass

def __safe_close(f: IO):
    try:
        if f is not None:
            f.close()
    except OSError:
        pass

def __execute_daemon_with_retry(daemon_main, daemon_args, logger, restart_interval):
    logger.debug("__execute_daemon_with_retry(guardian): enter")
    while not __quit_requested():
        executor_status, exit_code = __execute_daemon(daemon_main, daemon_args, logger)
        if executor_status == ExecutorStatus.FORK_FAILED:
            # no retry, should investigate why fork failed
            break
        if executor_status == ExecutorStatus.FINISHED:
            if restart_interval is None or exit_code == 0:
                break
            seconds_to_sleep = math.ceil(restart_interval.total_seconds())
            logger.debug(f"__execute_daemon_with_retry: retry after {seconds_to_sleep} seconds")
            sleep_if(seconds_to_sleep, lambda: not __quit_requested())
            continue
    logger.debug("__execute_daemon_with_retry(guardian): exit")

def __execute_daemon(daemon_main, daemon_args, logger):
    logger.debug("__execute_daemon(guardian): enter")
    new_pid = os.fork()

    if new_pid < 0:
        logger.debug("__execute_daemon(guardian): unable to launch executor")
        logger.debug("__execute_daemon(guardian): exit")
        return ExecutorStatus.FORK_FAILED, None

    if new_pid > 0:
        logger.debug(f"__execute_daemon(guardian): executor(pid={new_pid}) launched, waiting for it to finish")
        __APP_CONTEXT['executor_pid'] = new_pid
        _, exit_code = os.waitpid(new_pid, 0)
        __APP_CONTEXT['executor_pid'] = None
        logger.debug(f"__execute_daemon(guardian): executor(pid={new_pid}) finished, exit_code={exit_code}")
        logger.debug("__execute_daemon(guardian): exit")
        return ExecutorStatus.FINISHED, exit_code

    # we are in executor now
    __APP_CONTEXT['is_guardian'] = False
    signal.signal(signal.SIGTERM, __request_executor_shutdown)  # regular kill command

    try:
        daemon_main(daemon_args, __quit_requested)
        sys.exit(0)
    except Exception:
        logger.exception(f"__execute_daemon(executor): failed")
        sys.exit(1)
    

########################################################################################
# daemon_entry method should has the following signature
#     method(darmon_args:Dict, quit_requested:Callable[[],bool]) ->None
########################################################################################
def run_daemon(*,
               pid_filename: str,                               # filename, for the pid file
               daemon_entry: str,                               # daemon entry function name
               daemon_args: Optional[dict] = None,              # args passed to daemon
               stdout_filename: Optional[str] = None,           # filename for the stdout
               stderr_filename: Optional[str] = None,           # filename for the stderr
               logging_config: Optional[dict] = None,           # log config
               restart_interval: Optional[timedelta] = None,    # if executor failed, how long show we wait?
               ) -> Tuple[DaemonRunStatus, Any]:
    """
    Launch a daemon
    :param pid_filename: The filename for the pid file, acting as a unique identifier for the daemon.
    :param daemon_entry:
        The daemon entry function name, e.g., "main:foo", here, main is the module name and foo is
        the method name.
    :param daemon_args:Arguments pass to the daemon.
    :param stdout_filename:A filename to store the stdout, if missing, will point to /dev/null
    :param stderr_filename:A filename to store the stderr, if missing, will point to /dev/null
    :param logging_config:Log config, if missing, we won't initialize logging.
    :param restart_interval, if None, we won't try to restart executor, otherwise, we will restart executor after this interval
    :return:
        A tuple, first element is the DaemonRunStatus, 2nd element depend on the first element
        e.g.,
            (DaemonRunStatus.LAUNCHED, pid)
            (DaemonRunStatus.ALREADY_RUNNING, pid)
            (DaemonRunStatus.REDIR_STDOUT_FAILED, None)
            (DaemonRunStatus.REDIR_STDERR_FAILED, None)
            (DaemonRunStatus.FORK_FAILED, None)
    """
    if daemon_args is None:
        daemon_args = {}

    pid = __read_int_file(pid_filename)
    if pid is not None:
        # the daemon is already running
        return DaemonRunStatus.ALREADY_RUNNING, pid

    # Let's open the stdout and stderr
    if stdout_filename is None:
        stdout_filename = os.devnull
    if stderr_filename is None:
        stderr_filename = os.devnull

    out_f = None
    err_f = None
    in_f = None
    try:
        if stdout_filename == stderr_filename:
            # use the same file for stdout and stderr
            out_f = err_f = open(stdout_filename, "ab")
        else:
            out_f = open(stdout_filename, "ab")
            err_f = open(stderr_filename, "ab")
        in_f = open(os.devnull)
    except OSError:
        pass

    if out_f is None:
        __safe_close(in_f)
        return DaemonRunStatus.REDIR_STDOUT_FAILED, None
    if err_f is None:
        __safe_close(out_f)
        __safe_close(in_f)
        return DaemonRunStatus.REDIR_STDERR_FAILED, None

    new_pid = os.fork()
    if new_pid < 0:
        __safe_close(out_f)
        __safe_close(err_f)
        __safe_close(in_f)
        return DaemonRunStatus.FORK_FAILED, None

    if new_pid > 0:
        __safe_close(out_f)
        __safe_close(err_f)
        __safe_close(in_f)
        return DaemonRunStatus.LAUNCHED, new_pid

    # from here, we are in the child process
    initialize_ok = False
    try:
        # redirect stdout, stderr
        stdout_fn = sys.stdout.fileno()
        stderr_fn = sys.stderr.fileno()
        stdin_fn = sys.stdin.fileno()
        os.close(stdout_fn)
        os.close(stderr_fn)
        os.close(stdin_fn)
        os.dup2(out_f.fileno(), stdout_fn)
        os.dup2(err_f.fileno(), stderr_fn)
        os.dup2(in_f.fileno(), stdin_fn)
        out_f.close()
        err_f.close()
        in_f.close()

        os.setsid()
        os.umask(0)

        with open(pid_filename, "wt") as pid_f:
            print(os.getpid(), file=pid_f)

        if logging_config is not None:
            logging.config.dictConfig(logging_config)
        daemon_main = __get_method(daemon_entry)
        logger = logging.getLogger(__name__)

        initialize_ok = True
        logger.debug(f"""\
run_daemon(guardian): initialized{os.linesep}\
    pid_filename        = {pid_filename}
    daemon_entry        = {daemon_entry}
    daemon_args         = {daemon_args}
    stdout_filename     = {stdout_filename}
    stderr_filename     = {stderr_filename}
    restart_interval    = {restart_interval}""")
    finally:
        if not initialize_ok:
            __safe_remove(pid_filename)

    try:
        signal.signal(signal.SIGTERM, __request_guardian_shutdown)  # regular kill command
        __execute_daemon_with_retry(daemon_main, daemon_args, logger, restart_interval)
        # non guardian should not reach here
        logger.debug(f"run_daemon(guardian): exit")
        sys.exit(0)
    except Exception:
        # non guardian should not reach here
        logger.exception(f"run_daemon(guardian): failed")
        raise
    finally:
        if __APP_CONTEXT['is_guardian']:
            __safe_remove(pid_filename)


def _worker_entry(entry: str,
                  args: Dict,
                  status_quit_requested: Value,
                  logging_config: Optional[Dict] = None):
    if logging_config is not None:
        logging.config.dictConfig(logging_config)
    logger = logging.getLogger(__name__)
    entry_f = __get_method(entry)

    try:
        logger.info(f"{entry}: enter")
        entry_f(args, status_quit_requested)
        logger.info(f"{entry}: exit")
    except Exception:
        logger.exception(f"{entry} failed")
        raise


def run_worker(worker_info: List[Tuple[str, Optional[Dict], Optional[Dict]]],
               *,
               quit_requested: Callable[[], bool],
               check_interval: int = 1):
    sub_processes = []
    status_quit_requested = Value('b', False)

    for entry, args, logging_config in worker_info:
        if args is None:
            args = {}
        p = Process(
            target=_worker_entry,
            args=(entry, args, status_quit_requested, logging_config)
        )
        p.start()
        sub_processes.append(p)

    while not quit_requested():
        time.sleep(check_interval)

    status_quit_requested.value = True
    for p in sub_processes:
        p.join()

def daemon() -> None:
    sys.path.insert(0, '')          # add current directory
    default_log_config = {
        "version": 1,
        "disable_existing_loggers": True,
        "formatters": {
            "standard": {
                "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
            }
        },
        "handlers": {
            'console': {
                'level': 'INFO',
                'formatter': 'standard',
                'class': 'logging.StreamHandler',
                'stream': 'ext://sys.stdout'
            },
        },
        "root": {
            "handlers": ["console"],
            "level": "DEBUG",
        },
    }

    parser = argparse.ArgumentParser(
        description='Daemon Launch Tool'
    )
    parser.add_argument(
        "--log", type=str, required=False, help="Specify log config file",
    )
    parser.add_argument(
        "--pid", type=str, required=False, default="daemon.pid", help="Specify pid filename"
    )
    parser.add_argument(
        "--stdout", type=str, required=False, default="/dev/stdout", help="Specify stdout filename"
    )
    parser.add_argument(
        "--stderr", type=str, required=False, default="/dev/stderr", help="Specify stderr filename"
    )
    parser.add_argument(
        "-e", "--entry", type=str, required=True, help="Specify entry point"
    )
    args, _ = parser.parse_known_args()

    if args.log is None:
        log_config = default_log_config
    else:
        with open(args.log, "r") as f:
            log_config = json.load(f)

    status, extra = run_daemon(
        pid_filename    = args.pid,
        stdout_filename = args.stdout,
        stderr_filename = args.stderr,
        daemon_entry    = args.entry,
        logging_config  = log_config,
    )
    if status == DaemonRunStatus.LAUNCHED:
        print(f"Daemon launched, pid = {extra}, pid_filename = {args.pid}")
    else:
        print(f"Unable to launch daemon, status={status}, extra={extra}")
