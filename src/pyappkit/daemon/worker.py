from typing import List, Optional, Any
from datetime import timedelta, datetime
import logging
import math
import os
from multiprocessing import Process, Value
import json

from .tools import DT_FORMAT, SIG_TERM_HANDLER, sleep, safe_remove, redirect_io, get_method, ProcessRole

class WorkerStartInfo:
    pid_filename: str
    entry: str
    name: str
    args: dict
    logging_config: dict
    stdout_filename: str
    stderr_filename: str

    def __init__(
        self,
        *,
        pid_filename:str,
        entry:str,
        name:str,
        logging_config:dict,
        args:dict=dict(),
        stdout_filename:str=os.devnull,
        stderr_filename:str=os.devnull
    ):
        self.pid_filename = pid_filename
        self.entry = entry
        self.name = name
        self.args = args
        self.logging_config = logging_config
        self.stdout_filename = stdout_filename
        self.stderr_filename = stderr_filename

class WorkerHistoryInfo:
    pid: int
    exitcode: int
    start_time: datetime
    end_time: datetime

    def __init__(self, *, pid:int, exitcode:int, start_time:datetime, end_time:datetime):
        self.pid = pid
        self.exitcode = exitcode
        self.start_time = start_time
        self.end_time = end_time

    def to_json(self) -> Any:
        return {
            "pid": self.pid,
            "exitcode": self.exitcode,
            "start_time": None if self.start_time is None else self.start_time.strftime(DT_FORMAT),
            "end_time": None if self.end_time is None else self.end_time.strftime(DT_FORMAT)
        }

class WorkerInfo:
    index: int
    pid_filename: str
    entry: str
    name: str
    args: dict
    logging_config: dict
    stdout_filename: str
    stderr_filename: str
    process: Optional[Process]
    start_after: Optional[datetime]
    start_time: Optional[datetime]
    history: List[WorkerHistoryInfo]

    def __init__(self, index:int, worker_start_info:WorkerStartInfo):
        self.index = index
        self.pid_filename = worker_start_info.pid_filename
        self.entry = worker_start_info.entry
        self.name = worker_start_info.name
        self.args = worker_start_info.args
        self.logging_config = worker_start_info.logging_config
        self.stdout_filename = worker_start_info.stdout_filename
        self.stderr_filename = worker_start_info.stderr_filename
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
            "name": self.name,
            "stdout_filename": self.stdout_filename,
            "stderr_filename": self.stderr_filename,
            "pid": None if self.process is None else self.process.pid,
            "start_after": None if self.start_after is None else self.start_after.strftime(DT_FORMAT),
            "start_time": None if self.start_time is None else self.start_time.strftime(DT_FORMAT),
            "history": [item.to_json() for item in self.history]
        }

#########################################################################################################
# run a worker pool with fixed number of workers (not elastic)
# Restart Worker
# - Controled by restart_interval, restart worker feature will be disabled if restart_interval is None
# - Otherwise, it will try to restart a failed worker after restart_interval since we detect a worker
#   failed
#########################################################################################################
def start_workers(
    worker_start_infos: List[WorkerStartInfo],
    *,
    debug_filename:Optional[str],
    check_interval:timedelta=timedelta(minutes=1),
    restart_interval:timedelta=timedelta(minutes=5)
)->bool:
    logger = logging.getLogger(__name__)
    log_prefix = "[executor] start_workers"

    # make sure worker name are unique
    if len(set([worker_start_info.name for worker_start_info in worker_start_infos])) < len(worker_start_infos):
        logger.error(f"{log_prefix}: Duplicate worker names detected")
        return False

    check_interval_seconds = math.ceil(check_interval.total_seconds())

    worker_info_list = []
    for index, worker_start_info in enumerate(worker_start_infos):
        worker_info = WorkerInfo(index, worker_start_info)
        worker_info_list.append(worker_info)

    SIG_TERM_HANDLER.worker_controller = Value('b', False)

    while True:
        if SIG_TERM_HANDLER.quit_requested:
            logger.debug(f"{log_prefix}: bail out since quit requested")
            break

        worker_changed = False
        for worker_info in worker_info_list:
            if worker_info.is_dead() and not SIG_TERM_HANDLER.quit_requested:
                worker_changed = True
                worker_info.process.join()
                now = datetime.utcnow()
                worker_info.history.append(WorkerHistoryInfo(
                    pid = worker_info.process.pid,
                    exitcode= worker_info.process.exitcode,
                    start_time=worker_info.start_time,
                    end_time = now
                ))
                logger.debug(f"{log_prefix}: worker[{worker_info.index}]({worker_info.process.pid}): terminated, exitcode={worker_info.process.exitcode}")
                worker_info.process = None
                worker_info.start_after = None if restart_interval is None else now + restart_interval
                worker_info.start_time = None

            if worker_info.can_start() and not SIG_TERM_HANDLER.quit_requested:
                worker_changed = True
                p =  Process(
                    target=worker_main,
                    args=(
                        worker_info.pid_filename,
                        worker_info.entry,
                        worker_info.args,
                        worker_info.stdout_filename,
                        worker_info.stderr_filename,
                        SIG_TERM_HANDLER.worker_controller,
                        worker_info.logging_config
                    )
                )
                p.start()
                logger.debug(f"{log_prefix}: worker[{worker_info.index}]({p.pid}): created")
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

        sleep(check_interval_seconds)

    # if we quit the loop, we must have received SIGTERM signal
    for worker_info in worker_info_list:
        if worker_info.process is None:
            logger.debug(f"{log_prefix}: worker[{worker_info.index}]: already terminated")
        else:
            logger.debug(f"{log_prefix}: worker[{worker_info.index}]({worker_info.process.pid}): wait until finish")
            p.join()

    logger.debug(f"{log_prefix}: exit")
    return True


def worker_main(
    pid_filename:str,
    entry:str,
    args:dict,
    stdout_filename:str,
    stderr_filename:str,
    worker_controller: Value,
    logging_config:dict
):
    ###########################################################
    #
    # Entry point of worker
    #
    ###########################################################
    log_prefix = "[worker]"
    logging.config.dictConfig(logging_config)
    logger = logging.getLogger(__name__)

    try:
        with open(pid_filename, "wt") as pid_f:
            print(os.getpid(), file=pid_f)

        if not redirect_io(stdout_filename, stderr_filename):
            logger.error(f"{log_prefix}: Unable to redirect I/O, daemon terminated")
            return

        SIG_TERM_HANDLER.role = ProcessRole.WORKER
        SIG_TERM_HANDLER.worker_controller = worker_controller
        SIG_TERM_HANDLER.register()

        worker_method = get_method(entry)
        worker_method(**args)
        logger.debug(f"{log_prefix}: {entry} succeeded")
    except Exception:
        logger.exception(f"{log_prefix}: {entry} failed")
    finally:
        safe_remove(pid_filename)
