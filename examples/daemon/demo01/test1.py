#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import os
from pyappkit import start_daemon, DaemonRunStatus, sleep, quit_requested
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

import logging
logger = logging.getLogger(__name__)

def daemon_main(*, foo:int, bar:int):
    logger.debug(f"foo={foo}, bar={bar}")
    while True:
        logger.debug("I am running")
        sleep(25)
        if quit_requested():
            break


def main():
    os.makedirs(".data", exist_ok=True)
    status, pid = start_daemon(
        pid_filename=".data/foo.pid",
        stdout_filename=".data/out.txt",
        stderr_filename=".data/err.txt",
        daemon_entry="test1:daemon_main",
        logging_config=LOG_CONFIG,
        daemon_args=dict(foo=1, bar=2),
        restart_interval=timedelta(seconds=10)
    )
    if status == DaemonRunStatus.LAUNCHED:
        print(f"Daemon launched, pid = {pid}")
    else:
        print(f"Unable to launch daemon, status={status}, extra={pid}")


if __name__ == '__main__':
    main()
