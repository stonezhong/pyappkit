import logging
logger = logging.getLogger(__name__)

from multiprocessing import set_start_method, Value
import time
from typing import Dict, Callable, Any
from pyappkit import run_workers, WorkerStartInfo, Executable
from datetime import datetime, timedelta

class MyDaemon(Executable):
    def run(self, foo:int, bar:int):
        logger.info(f"foo={foo}, bar={bar}")
        run_workers(
            [
                WorkerStartInfo(
                    pid_filename=".data/worker-1.pid",
                    entry="daemon_impl:MyWorker",
                    name="foo",
                    args={},
                    logging_config=get_log_config(1),
                    stdout_filename=".data/w1.out",
                    stderr_filename=".data/w1.err"
                ),
                WorkerStartInfo(
                    pid_filename=".data/worker-2.pid",
                    entry="daemon_impl:MyWorker",
                    name="bar",
                    args={},
                    logging_config=get_log_config(2),
                    stdout_filename=".data/w2.out",
                    stderr_filename=".data/w2.err"
                ),
            ],
            debug_filename=".data/debug.json",
            check_interval=timedelta(seconds=5),
            restart_interval=timedelta(seconds=15)
        )
        logger.info("daemon is finished!")

class MyWorker(Executable):
    def run(self):
        while not self.quit_requested():
            print(f"Hello, {datetime.utcnow()}", flush=True)
            logger.info("I am running...")
            self.sleep(timedelta(seconds=10))
        logger.info("I am done")


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


