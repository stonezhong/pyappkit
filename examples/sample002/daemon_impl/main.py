import logging
logger = logging.getLogger(__name__)

from multiprocessing import set_start_method, Value
import time
from typing import Dict, Callable, Any
from pyappkit import run_worker, Worker
from datetime import datetime, timedelta


def get_log_config(i):
    return {
        "version": 1,
        "disable_existing_loggers": True,
        "formatters": {
            "standard": {
                "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s"
            }
        },
        "handlers": {
            "fileHandler": {
                "class": "logging.handlers.TimedRotatingFileHandler",
                "level": "DEBUG",
                "formatter": "standard",
                "filename": f".data/test2_worker-{i}.log",
                "interval": 1,
                "when": "midnight"
            },
        },
        "root": {
            "handlers": ["fileHandler"],
            "level": "DEBUG",
        },
    }


def worker_main(args: Any, quit_requested: Callable[[], bool]) -> None:
    while not quit_requested():
        print(f"Hello, {datetime.utcnow()}", flush=True)
        logger.info("I am running...")
        time.sleep(1)

    logger.info("I am done")


def main(daemon_args: Any, quit_requested: Callable[[], bool]) -> None:
    set_start_method("spawn", force=True)

    logger.info(daemon_args)

    run_worker(
        [
            Worker(entry="daemon_impl:worker_main", args={}, logging_config=get_log_config(1), stdout_filename=".data/w1.out",stderr_filename=".data/w1.err"),
            Worker(entry="daemon_impl:worker_main", args={}, logging_config=get_log_config(2), stdout_filename=".data/w2.out",stderr_filename=".data/w2.err"),
        ],
        debug_filename=".data/debug.json",
        check_interval=timedelta(seconds=5),
        restart_interval=timedelta(seconds=15)
    )

    logger.info("daemon is finished!")
