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
from datetime import timedelta, datetime
import math

from multiprocessing import Process, Value

DT_FORMAT = "%Y-%m-%d %H:%M:%S.%f"

class DaemonRunStatus(Enum):
    LAUNCHED = 1
    ALREADY_RUNNING = 2
    REDIR_STDOUT_FAILED = 3
    REDIR_STDERR_FAILED = 4
    FORK_FAILED = 5

class ExecutorStatus(Enum):
    FORK_FAILED = 1
    FINISHED = 2

class AppContext:
    quit_requested: bool
    executor_pid: Optional[int]
    guardian_pid: Optional[int]
    is_guardian: bool
    worker_controller: Optional[Value]

    def __init__(self, is_guardian:bool):
        self.quit_requested = False
        self.is_guardian = is_guardian
        self.executor_pid = None
        self.guardian_pid = None
        self.worker_controller = None

__APP_CONTEXT = AppContext(is_guardian=True)

def __request_guardian_shutdown(signal_umber, frame):
    _, _ = signal_umber, frame
    __APP_CONTEXT.quit_requested = True
    __safe_kill(__APP_CONTEXT.executor_pid)

def __request_executor_shutdown(signal_umber, frame):
    _, _ = signal_umber, frame
    __APP_CONTEXT.quit_requested = True
    if __APP_CONTEXT.worker_controller is not None:
        __APP_CONTEXT.worker_controller.value = True
    __safe_kill(__APP_CONTEXT.guardian_pid)


def __quit_requested():
    return __APP_CONTEXT.quit_requested


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
        if pid is not None:
            os.kill(pid, signal.SIGTERM)
    except OSError:
        pass

def __safe_close(f: IO):
    try:
        if f is not None:
            f.close()
    except OSError:
        pass

def __safe_close_io(in_f:IO, out_f: IO, err_f: IO):
    if out_f is err_f:
        __safe_close(out_f)
    else:
        __safe_close(out_f)
        __safe_close(err_f)
    __safe_close(in_f)

def __execute_daemon_with_retry(daemon_main, daemon_args, logger, restart_interval):
    executor_launch_count = 0
    logger.debug("__execute_daemon_with_retry(guardian): enter")
    seconds_to_sleep = None if restart_interval is None else math.ceil(restart_interval.total_seconds())
    while True:
        if __quit_requested():
            logger.debug(f"__execute_daemon_with_retry(guardian): executor_launch_count={executor_launch_count}, bail out since quit requested")
            break
        logger.debug(f"__execute_daemon_with_retry(guardian): executor_launch_count={executor_launch_count}, launch executor")
        executor_status, exit_code = __execute_daemon(daemon_main, daemon_args, logger)
        executor_launch_count += 1
        if executor_status == ExecutorStatus.FORK_FAILED:
            # no retry, should investigate why fork failed
            logger.debug(f"__execute_daemon_with_retry(guardian): executor_launch_count={executor_launch_count}, bail out since we are unable to launch executor")
            break
        if executor_status == ExecutorStatus.FINISHED:
            if restart_interval is None or exit_code == 0:
                logger.debug(f"__execute_daemon_with_retry(guardian): executor_launch_count={executor_launch_count}, bail out since executor completed successfully")
                break
            logger.debug(f"__execute_daemon_with_retry(guardian): executor_launch_count={executor_launch_count}, executor failed with {exit_code}, sleep for {seconds_to_sleep} seconds")
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
        __APP_CONTEXT.executor_pid = new_pid
        _, exit_code = os.waitpid(new_pid, 0)
        __APP_CONTEXT.executor_pid = None
        logger.debug(f"__execute_daemon(guardian): executor(pid={new_pid}) finished, exit_code={exit_code}")
        logger.debug("__execute_daemon(guardian): exit")
        return ExecutorStatus.FINISHED, exit_code

    # we are in executor now
    __APP_CONTEXT.is_guardian = False
    __APP_CONTEXT.executor_pid = os.getpid()
    __APP_CONTEXT.guardian_pid = os.getppid()
    signal.signal(signal.SIGTERM, __request_executor_shutdown)  # regular kill command

    try:
        daemon_main(daemon_args, __quit_requested)
        sys.exit(0)
    except Exception:
        logger.exception(f"__execute_daemon(executor): failed")
        sys.exit(1)


#########################################################################################################
# daemon_entry method should has the following signature
#     method(darmon_args:Dict, quit_requested:Callable[[],bool]) ->None
# ------------------------------------------------------------------------------------------------------
# - Both guardian and executor will try to kill each other upon receiving SIGTERM signal
#   - If you kill (SIGTERM) an executor, the executor will send a SIGTERM to guardian, guardian will
#     wait for executor to terminate and then quit, without trying to restart executor
#   - If you kill (SIGTERM) guardian, it will send a SIGTERM to executor, and wait for the executor to
#     terminate and then quit, without trying to restart executor
# - user code (from daemon_entry) should constantly pull quit_requested() and return from the method
#   as early as possible if quit_requested() returns True
# - If restart_interval is None, the guardian will never seek to restart executor, instead, the guardian
#   simply wait for the executor to terminate and then quit.
#########################################################################################################
def run_daemon(
    *,
    pid_filename:str,                                # filename, for the pid file
    daemon_entry:str,                                # daemon entry function name
    daemon_args:Any=None,                            # args passed to daemon
    stdout_filename:Optional[str]=None,              # filename for the stdout
    stderr_filename:Optional[str]=None,              # filename for the stderr
    logging_config:Optional[dict]=None,              # log config
    restart_interval:Optional[timedelta]=None,       # if executor failed, how long show we wait?
) -> Tuple[DaemonRunStatus, Any]:
    """
    Launch a daemon
    :param pid_filename: The filename for the pid file, acting as a unique identifier for the daemon.
    :param daemon_entry:
        The daemon entry function name, e.g., "main:foo", here, main is the module name and foo is
        the method name.
    :param daemon_args:Arguments pass to the daemon.
    :param stdout_filename: Filename to store the stdout, if missing, will point to /dev/null
    :param stderr_filename: Filename to store the stderr, if missing, will point to /dev/null
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
            out_f = err_f = open(stdout_filename, "wb")
        else:
            out_f = open(stdout_filename, "wb")
            err_f = open(stderr_filename, "wb")
        in_f = open(os.devnull)
    except OSError:
        pass

    if out_f is None:
        __safe_close_io(in_f, out_f, err_f)
        return DaemonRunStatus.REDIR_STDOUT_FAILED, None

    if err_f is None:
        __safe_close_io(in_f, out_f, err_f)
        return DaemonRunStatus.REDIR_STDERR_FAILED, None

    new_pid = os.fork()
    if new_pid < 0:
        __safe_close_io(in_f, out_f, err_f)
        return DaemonRunStatus.FORK_FAILED, None

    if new_pid > 0:
        __safe_close_io(in_f, out_f, err_f)
        return DaemonRunStatus.LAUNCHED, new_pid

    # from here, we are in the child process
    __APP_CONTEXT.guardian_pid = os.getpid()
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
        if __APP_CONTEXT.is_guardian:
            __safe_remove(pid_filename)


def __worker_wrapper(entry:str, args:Any, stdout_filename:str, stderr_filename:str, worker_controller: Value, logging_config:dict):
    logging.config.dictConfig(logging_config)
    logger = logging.getLogger(__name__)

    logger.debug(f"""{entry}: enter
    entry          : {entry}
    args           : {args}
    stdout_filename: {stdout_filename}
    stderr_filename: {stderr_filename}""")

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
            out_f = err_f = open(stdout_filename, "wb")
        else:
            out_f = open(stdout_filename, "wb")
            err_f = open(stderr_filename, "wb")
        in_f = open(os.devnull)
    except OSError:
        pass

    if out_f is None:
        __safe_close_io(in_f, out_f, err_f)
        logger.debug(f"{entry}: unable to open {stdout_filename}")
        logger.debug(f"{entry}: exit")
        sys.exit(1)

    if err_f is None:
        __safe_close_io(in_f, out_f, err_f)
        logger.debug(f"{entry}: unable to open {stderr_filename}")
        logger.debug(f"{entry}: exit")
        sys.exit(1)


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

    try:
        entry_f = __get_method(entry)
        entry_f(args, lambda:worker_controller.value)
        logger.debug(f"{entry}: exit")
    except Exception:
        logger.exception(f"{entry} failed")
        raise

class WorkerHistoryInfo:
    pid: int
    exit_code: int
    start_time: datetime
    end_time: datetime

    def __init__(self, *, pid:int, exit_code:int, start_time:datetime, end_time:datetime):
        self.pid = pid
        self.exit_code = exit_code
        self.start_time = start_time
        self.end_time = end_time

    def to_json(self) -> Any:
        return {
            "pid": self.pid,
            "exit_code": self.exit_code,
            "start_time": None if self.start_time is None else self.start_time.strftime(DT_FORMAT),
            "end_time": None if self.end_time is None else self.end_time.strftime(DT_FORMAT)
        }

class WorkerInfo:
    index: int
    entry: Optional[str]
    args: Any
    logging_config: Optional[dict]
    stdout_filename: Optional[str]
    stderr_filename: Optional[str]
    process: Optional[Process]
    start_after: Optional[datetime]
    start_time: Optional[datetime]
    history: List[WorkerHistoryInfo]

    def __init__(self, index:int):
        self.index = index
        self.entry = None
        self.args = None
        self.logging_config = None
        self.stdout_filename = None
        self.stderr_filename = None
        self.process = None
        self.start_after = None
        self.start_time = None
        self.history = []

    def is_dead(self):
        return self.process is not None and not self.process.is_alive()

    def can_start(self):
        if self.process is not None:
            # need to wait for current process to quit before you can start it
            return False

        if len(self.history) == 0:
            # you can always start a worker if it has never been started
            return True

        # time is due
        return self.start_after is not None and datetime.utcnow() >= self.start_after

    def to_json(self) -> Any:
        return {
            "index": self.index,
            "entry": self.entry,
            "stdout_filename": self.stdout_filename,
            "stderr_filename": self.stderr_filename,
            "pid": None if self.process is None else self.process.pid,
            "start_after": None if self.start_after is None else self.start_after.strftime(DT_FORMAT),
            "start_time": None if self.start_time is None else self.start_time.strftime(DT_FORMAT),
            "history": [item.to_json() for item in self.history]
        }

class Worker:
    entry: str
    args: Any
    logging_config: dict
    stdout_filename: Optional[str]
    stderr_filename: Optional[str]

    def __init__(self, *, entry:str, logging_config:dict, args:Any=None, stdout_filename:str=None, stderr_filename:str=None):
        self.entry = entry
        self.logging_config = logging_config
        self.args = args
        self.stdout_filename = stdout_filename
        self.stderr_filename = stderr_filename

#########################################################################################################
# run a worker pool with fixed number of workers (not elastic)
# Restart Worker
# - Controled by restart_interval, restart worker feature will be disabled if restart_interval is None
# - Otherwise, it will try to restart a failed worker after restart_interval since we detect a worker
#   failed
#########################################################################################################
def run_worker(
    workers: List[Worker],
    *,
    debug_filename:Optional[str],
    check_interval:timedelta=timedelta(minutes=1),
    restart_interval:timedelta=timedelta(minutes=5)
):
    logger = logging.getLogger(__name__)
    logger.debug("run_worker: enter")
    check_interval_seconds = math.ceil(check_interval.total_seconds())

    worker_info_list = []
    for index, worker in enumerate(workers):
        worker_info = WorkerInfo(index)
        worker_info.entry = worker.entry
        worker_info.args = worker.args
        worker_info.logging_config = worker.logging_config
        worker_info.stdout_filename = worker.stdout_filename
        worker_info.stderr_filename = worker.stderr_filename
        worker_info_list.append(worker_info)

    worker_controller = Value('b', False)
    __APP_CONTEXT.worker_controller = worker_controller

    while True:
        if __quit_requested():
            logger.debug(f"run_worker: bail out since quit requested")
            break

        worker_changed = False
        for worker_info in worker_info_list:
            if worker_info.is_dead() and not __quit_requested():
                worker_changed = True
                worker_info.process.join()
                now = datetime.utcnow()
                worker_info.history.append(WorkerHistoryInfo(
                    pid = worker_info.process.pid,
                    exit_code= worker_info.process.exitcode,
                    start_time=worker_info.start_time,
                    end_time = now
                ))
                logger.debug(f"run_worker: worker[{worker_info.index}]({worker_info.process.pid}): terminated unexpectedly, exitcode={worker_info.process.exitcode}")
                worker_info.process = None
                worker_info.start_after = None if restart_interval is None else now + restart_interval
                worker_info.start_time = None

            if worker_info.can_start() and not __quit_requested():
                worker_changed = True
                p =  Process(
                    target=__worker_wrapper,
                    args=(
                        worker_info.entry,
                        worker_info.args,
                        worker_info.stdout_filename,
                        worker_info.stderr_filename,
                        worker_controller,
                        worker_info.logging_config
                    )
                )
                p.start()
                logger.debug(f"run_worker: worker[{worker_info.index}]({p.pid}): created")
                worker_info.process = p
                worker_info.start_time = datetime.utcnow()

        if worker_changed and debug_filename is not None:
            with open(debug_filename, "wt") as f:
                payload = {
                    "updated_at": datetime.utcnow().strftime(DT_FORMAT),
                    "worker_info_list": [worker_info.to_json() for worker_info in worker_info_list]
                }
                json.dump(payload, f)
                logger.debug(f"run_worker: debug info dummped")

        logger.debug(f"run_worker: sleep for {check_interval_seconds} seconds")
        sleep_if(check_interval_seconds, lambda: not __quit_requested())

    # if we quit the loop, we must have received SIGTERM signal
    for worker_info in worker_info_list:
        if worker_info.process is None:
            logger.debug(f"run_worker: worker[{worker_info.index}]: already terminated")
        else:
            logger.debug(f"run_worker: worker[{worker_info.index}]({worker_info.process.pid}): wait until finish")
            p.join()

    logger.debug("run_worker: exit")

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
