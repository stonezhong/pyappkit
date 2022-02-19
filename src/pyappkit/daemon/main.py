import importlib
import os
import sys
import signal
import os
import logging
import logging.config
import time
from enum import Enum
from typing import Tuple, Any, Dict


class DaemonRunStatus(Enum):
    LAUNCHED            = 1
    ALREADY_RUNNING     = 2
    REDIR_STDOUT_FAILED = 3
    REDIR_STDERR_FAILED = 4
    FORK_FAILED         = 5


__APP_CONTEXT = {
    'quit_requested': False
}

def __request_shutdown(signalNumber, frame):
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


def __read_int_file(filename:str)->int:
    if not os.path.isfile(filename):
        return None
    with open(filename, "rt") as f:
        return int(f.read())

def __save_remove(filename:str):
    try:
        os.remove(filename)
    except:
        pass

########################################################################################
# daemon_entry method should has the following signature
#     method(darmon_args:Dict, quit_requested:Callable[[],bool]) ->None
# exception handler should have the following signature
#     handle_exception(ex:Exception, daemon_args:Dict):
########################################################################################
def run_daemon(
    *,
    pid_filename:str,               # filename, for the pid file
    stdout_filename:str=None,       # filename for the stdout
    stderr_filename:str=None,       # filename for the stderr
    logging_config:dict,            # log config
    daemon_entry:str,               # daemon entry function name
    exception_handler_entry:str,    # daemon entry function name
    daemon_args:Dict,               # args passed to daemon
) -> Tuple[DaemonRunStatus, Any]:
    # A daemon MUST cleanup the pid file upon exit
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
    in_f  = None
    try:
        if stdout_filename == stderr_filename:
            # use the same file for stdout and stderr
            out_f = err_f = open(stdout_filename, "ab")
        else:
            out_f = open(stdout_filename, "ab")
            err_f = open(stderr_filename, "ab")
        in_f = open(os.devnull)
    except:
        pass
    
    if out_f is None:
        in_f.close()
        return DaemonRunStatus.REDIR_STDOUT_FAILED, None
    if err_f is None:
        out_f.close()
        in_f.close()
        return DaemonRunStatus.REDIR_STDERR_FAILED, None

    new_pid = os.fork()
    if new_pid < 0:
        out_f.close()
        err_f.close()
        in_f.close()
        return DaemonRunStatus.FORK_FAILED, None

    if new_pid > 0:
        out_f.close()
        err_f.close()
        in_f.close()
        return DaemonRunStatus.LAUNCHED, new_pid


    # from here, we are in the child process
    initialize_ok       = False
    logger              = None
    daemon_main         = None
    exception_handler   = None
    try:
        # redirect stdout, stderr
        stdout_fn = sys.stdout.fileno()
        stderr_fn = sys.stderr.fileno()
        stdin_fn  = sys.stdin.fileno()
        os.close(stdout_fn)
        os.close(stderr_fn)
        os.close(stdin_fn)
        os.dup2(out_f.fileno(), stdout_fn)
        os.dup2(err_f.fileno(), stderr_fn)
        os.dup2(in_f.fileno(),  stdin_fn)
        out_f.close()
        err_f.close()
        in_f.close()

        os.setsid()
        os.umask(0)

        with open(pid_filename, "wt") as pid_f:
            print(os.getpid(), file=pid_f)

        logging.config.dictConfig(logging_config)
        exception_handler   = __get_method(exception_handler_entry)
        daemon_main         = __get_method(daemon_entry)
        logger = logging.getLogger(__name__)

        initialize_ok       = True
        logging.info(f"daemon {pid_filename}: initialized")
    finally:
        if not initialize_ok:
            __save_remove(pid_filename)

    try:
        signal.signal(signal.SIGTERM, __request_shutdown)   # regular kill command
        logger.info(f"{pid_filename}: enter")
        daemon_main(daemon_args, __quit_requested)
        logger.info(f"{pid_filename}: exit")
    except Exception as ex:
        logger.exception(f"{pid_filename}: failed")
        exception_handler(ex, daemon_args)
    finally:
        __save_remove(pid_filename)
