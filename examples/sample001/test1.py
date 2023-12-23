#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import logging
from typing import Callable
import os
from pyappkit.daemon2 import start_daemon, DaemonRunStatus, start_workers, WorkerStartInfo, sleep, quit_requested
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
        },
        "fileSysHandler": {
            "class": "logging.handlers.TimedRotatingFileHandler",
            "level": "DEBUG",
            "formatter": "standard",
            "filename": ".data/sys.log",
            "interval": 1,
            "when": "midnight"
        }
    },
    "root": {
        "handlers": ["fileHandler"],
        "level": "DEBUG",
    },
    "loggers": {
        "pyappkit.daemon": {
            "handlers": ["fileSysHandler"],
            "level": "DEBUG"
        }
    }
}

def get_worker_log_config(id)->dict:
    return {
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

def daemon1(*, foo:int, bar:int):
    logger.debug(f"foo={foo}, bar={bar}")
    while True:
        logger.debug("I am running")
        sleep(25)
        if quit_requested():
            break

def daemon2(*, foo:int, bar:int):
    start_workers(
        [
            WorkerStartInfo(
                pid_filename=".data/worker-1.pid",
                entry="test1:worker",
                name="worker-1",
                logging_config=get_worker_log_config(1),
                args={"color": "red"},
                stdout_filename = ".data/worker-1-out.txt",
                stderr_filename = ".data/worker-1-err.txt",
            ),
            WorkerStartInfo(
                pid_filename=".data/worker-2.pid",
                entry="test1:worker",
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
        daemon_entry="test1:daemon1",
        logging_config=LOG_CONFIG,
        daemon_args=dict(foo=1, bar=2),
        # restart_interval=timedelta(seconds=10)
    )
    if status == DaemonRunStatus.LAUNCHED:
        print(f"Daemon launched, pid = {pid}")
    else:
        print(f"Unable to launch daemon, status={status}, extra={pid}")


if __name__ == '__main__':
    main()

