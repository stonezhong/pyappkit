import logging
logger = logging.getLogger(__name__)

from multiprocessing import set_start_method, Value
import time
from typing import Dict, Callable
from pyappkit import run_worker
from datetime import datetime


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
                "filename": f".data/test_worker-{i}.log",
                "interval": 1,
                "when": "midnight"
            },
        },
        "root": {
            "handlers": ["fileHandler"],
            "level": "DEBUG",
        },
    }


def worker_main(args: Dict, status_quit_requested: Value) -> None:
    while not status_quit_requested.value:
        print(f"Hello, {datetime.utcnow()}", flush=True)
        logger.info("I am running...")
        time.sleep(1)

    logger.info("I am done")


def main(daemon_args: Dict, quit_requested: Callable[[], bool]) -> None:
    set_start_method("spawn")

    logger.info(daemon_args)

    run_worker(
        [
            ("daemon_impl:worker_main", {}, get_log_config(1)),
            ("daemon_impl:worker_main", {}, get_log_config(2)),
        ],
        quit_requested=quit_requested,
    )

    logger.info("daemon is finished!")
