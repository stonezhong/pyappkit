#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import logging
from typing import Callable
import os
from pyappkit import start_daemon, DaemonRunStatus, start_workers, WorkerStartInfo, sleep, quit_requested
from datetime import timedelta

LOG_CONFIG = {
    "version": 1,
    "disable_existing_loggers": False,
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
            "filename": ".data/test.log",
            "interval": 1,
            "when": "midnight"
        }
    },
    "root": {
        "handlers": ["fileHandler"],
        "level": "DEBUG",
    }
}

def get_worker_log_config(id)->dict:
    return \
    {
        "version": 1,
        "disable_existing_loggers": False,
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
                "filename": f".data/worker-{id}.log",
                "interval": 1,
                "when": "midnight"
            }
        },
        "root": {
            "handlers": ["fileHandler"],
            "level": "DEBUG",
        }
    }

logger = logging.getLogger(__name__)

def daemon_main(*, foo:int, bar:int):
    start_workers(
        [
            WorkerStartInfo(
                pid_filename=".data/worker-1.pid",
                entry="test2:worker",
                name="worker-1",
                logging_config=get_worker_log_config(1),
                args={"color": "red"},
                stdout_filename = ".data/worker-1-out.txt",
                stderr_filename = ".data/worker-1-err.txt",
            ),
            WorkerStartInfo(
                pid_filename=".data/worker-2.pid",
                entry="test2:worker",
                name="worker-2",
                logging_config=get_worker_log_config(2),
                args={"color": "yellow"},
                stdout_filename = ".data/worker-2-out.txt",
                stderr_filename = ".data/worker-2-err.txt",
            )
        ],
        debug_filename=".data/debug.json",
        check_interval=timedelta(seconds=5),
        restart_interval=timedelta(seconds=20)
    )


def main():
    os.makedirs(".data", exist_ok=True)
    status, pid = start_daemon(
        pid_filename=".data/foo.pid",
        stdout_filename=".data/out.txt",
        stderr_filename=".data/err.txt",
        daemon_entry="test2:daemon_main",
        logging_config=LOG_CONFIG,
        daemon_args=dict(foo=1, bar=2),
        restart_interval=timedelta(seconds=10)
    )
    if status == DaemonRunStatus.LAUNCHED:
        print(f"Daemon launched, pid = {pid}")
    else:
        print(f"Unable to launch daemon, status={status}, extra={pid}")

def worker(*, color:str):
    logger.debug(f"color={color}")
    while not quit_requested():
        logger.info("worker is running")
        sleep(5)


if __name__ == '__main__':
    main()

