from typing import Optional, Any, Tuple
from datetime import timedelta
from enum import Enum
import os
import sys
from multiprocessing import Process, set_start_method
import logging
import logging.config
import math

from .tools import read_int_file, redirect_io, safe_remove, get_method, SigTermHandler, ProcessRole, \
    sleep, SIG_TERM_HANDLER

class DaemonRunStatus(Enum):
    LAUNCHED = 1              # guardian has been started
    ALREADY_RUNNING = 2       # guardian is already running
    LAUNCH_FAILED = 3         # failed to start guardian

#########################################################################################################
# start_daemon                  API
# guardian_main                 In guardian, main entry of guardian
# launch_executor               In guardian, launch executor
# launch_executor_with_retry    In guardian, launch executor, handle error and retry logic
# executor_main                 In executor, main entry of executor
#########################################################################################################

#########################################################################################################
# daemon_entry method should point to a class that is derived from Daemon class
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
def start_daemon(
    *,
    pid_filename:str,                                # filename, for the pid file
    daemon_entry:str,                                # daemon entry function name
    daemon_args:dict=dict(),                         # args passed to daemon
    stdout_filename:str=os.devnull,                  # filename for the stdout
    stderr_filename:str=os.devnull,                  # filename for the stderr
    logging_config:Optional[dict]=None,              # log config
    restart_interval:Optional[timedelta]=None,       # if executor failed, how long show we wait?
) -> Tuple[DaemonRunStatus, Optional[int]]:
    """
    Launch a daemon
    :param pid_filename: The filename for the pid file, acting as a unique identifier for the daemon.
    :param daemon_entry:
        To daemon entry function., e.g., "mydaemon:MyDaemon", here, mydaemon is the module name and MyDaemon is
        the class name.
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
            (DaemonRunStatus.LAUNCH_FAILED, None)
    """
    pid = read_int_file(pid_filename)
    if pid is not None:
        return DaemonRunStatus.ALREADY_RUNNING, pid

    new_pid = os.fork()
    if new_pid < 0:
        return DaemonRunStatus.LAUNCH_FAILED, None

    if new_pid > 0:
        return DaemonRunStatus.LAUNCHED, new_pid

    try:
        guardian_main(
            pid_filename,
            daemon_entry,
            daemon_args,
            stdout_filename,
            stderr_filename,
            logging_config,
            restart_interval
        )
        # never return to the caller
        sys.exit(0)
    finally:
        safe_remove(pid_filename)

###########################################################
#
# Entry point of guardian
#
###########################################################
def guardian_main(
    pid_filename:str,                                # filename, for the pid file
    daemon_entry:str,                                # daemon entry function name
    daemon_args:dict=dict(),                         # args passed to daemon
    stdout_filename:str=os.devnull,                  # filename for the stdout
    stderr_filename:str=os.devnull,                  # filename for the stderr
    logging_config:Optional[dict]=None,              # log config
    restart_interval:Optional[timedelta]=None,       # if executor failed, how long show we wait?
):

    # TODO: check potential racing condition that a pid file is generated before guardian_main is called
    #       but after the pid check in start_daemon

    log_prefix = "[guardian]"
    # generate pid file
    with open(pid_filename, "wt") as pid_f:
        print(os.getpid(), file=pid_f)

    if logging_config is not None:
        logging.config.dictConfig(logging_config)
    logger = logging.getLogger(__name__)

    if not redirect_io(stdout_filename, stderr_filename):
        logger.error(f"{log_prefix}: Unable to redirect I/O, daemon terminated")
        return

    set_start_method("spawn")

    # register sigterm handler
    SIG_TERM_HANDLER.role = ProcessRole.GUARDIAN
    SIG_TERM_HANDLER.register()

    try:
        launch_executor_with_retry(
            logging_config,
            daemon_entry,
            daemon_args,
            logger,
            restart_interval
        )
    except Exception:
        logger.exception(f"{log_prefix}: Unable to launch executor!")

def launch_executor_with_retry(
    logging_config:dict,
    daemon_entry:str,
    daemon_args:dict,
    logger:logging.Logger,
    restart_interval:Optional[timedelta]
):
    log_prefix = "[guardian]"
    seconds_to_sleep = None if restart_interval is None else math.ceil(restart_interval.total_seconds())
    while True:
        if SIG_TERM_HANDLER.quit_requested:
            logger.debug(f"{log_prefix}: bail out launch executor loop since quit requested")
            break

        logger.debug(f"{log_prefix}: launching executor")
        p =  Process(
            target=executor_main,
            args=(
                os.getpid(),
                logging_config,
                daemon_entry,
                daemon_args,
            )
        )
        p.start()
        SIG_TERM_HANDLER.executor_pid = p.pid
        p.join()
        SIG_TERM_HANDLER.executor_pid = None
        logger.debug(f"{log_prefix}: executor finished, with {p.exitcode}")

        if p.exitcode == 0:
            logger.debug(f"{log_prefix}: bail out launch executor loop since executor finiished successfully")
            break

        if seconds_to_sleep is None:
            logger.debug(f"{log_prefix}: bail out launch executor loop since retry is disabled")
            break

        logger.debug(f"{log_prefix}: sleep {seconds_to_sleep} seconds before retry")
        sleep(seconds_to_sleep)
    logger.debug(f"{log_prefix}: exit")


def executor_main(guardian_pid:int, logging_config:dict, daemon_entry:str, daemon_args:dict):
    log_prefix = "[executor]"

    if logging_config is not None:
        logging.config.dictConfig(logging_config)
    logger = logging.getLogger(__name__)

    logger.debug(f"{log_prefix}: guardian_pid={guardian_pid}, daemon_entry={daemon_entry}, daemon_args={daemon_args}")

    # set_start_method("spawn")

    # register sigterm handler
    SIG_TERM_HANDLER.role = ProcessRole.EXECUTOR
    SIG_TERM_HANDLER.guardian_pid = guardian_pid
    SIG_TERM_HANDLER.register()

    try:
        daemon_method = get_method(daemon_entry)
        daemon_method(**daemon_args)
        logger.debug(f"{log_prefix}: {daemon_entry} succeeded")
        sys.exit(0)
    except Exception:
        logger.exception(f"{log_prefix}: {daemon_entry} failed")
        sys.exit(1)

