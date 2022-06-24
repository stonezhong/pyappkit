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


from multiprocessing import Process, Value


class DaemonRunStatus(Enum):
    LAUNCHED = 1
    ALREADY_RUNNING = 2
    REDIR_STDOUT_FAILED = 3
    REDIR_STDERR_FAILED = 4
    FORK_FAILED = 5


__APP_CONTEXT = {
    'quit_requested': False
}


def __request_shutdown(signal_umber, frame):
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


def __safe_close(f: IO):
    try:
        if f is not None:
            f.close()
    except OSError:
        pass


########################################################################################
# daemon_entry method should has the following signature
#     method(darmon_args:Dict, quit_requested:Callable[[],bool]) ->None
########################################################################################
def run_daemon(*,
               pid_filename: str,  # filename, for the pid file
               daemon_entry: str,  # daemon entry function name
               daemon_args: Optional[dict] = None,     # args passed to daemon
               stdout_filename: Optional[str] = None,  # filename for the stdout
               stderr_filename: Optional[str] = None,  # filename for the stderr
               logging_config: Optional[dict] = None,  # log config
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
    :return:
        A tuple, first element is the DaemonRunStatus, 2nd element depend on the first element
        e.g.,
            (DaemonRunStatus.LAUNCHED, pid)
            (DaemonRunStatus.ALREADY_RUNNING, pid)
            (DaemonRunStatus.REDIR_STDOUT_FAILED, None)
            (DaemonRunStatus.REDIR_STDERR_FAILED, None)
            (DaemonRunStatus.FORK_FAILED, None)
    """
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
        logger.info(f"daemon {pid_filename}: initialized")
    finally:
        if not initialize_ok:
            __safe_remove(pid_filename)

    try:
        signal.signal(signal.SIGTERM, __request_shutdown)  # regular kill command
        logger.info(f"{pid_filename}: enter")
        if daemon_args is None:
            daemon_args = {}
        daemon_main(daemon_args, __quit_requested)
        logger.info(f"{pid_filename}: exit")
        sys.exit(0)
    except Exception:
        logger.exception(f"{pid_filename}: failed")
        raise
    finally:
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
