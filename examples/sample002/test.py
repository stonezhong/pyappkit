#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import os
from pyappkit import run_daemon, DaemonRunStatus

LOG_CONFIG = {
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
            "filename": ".data/test2.log",
            "interval": 1,
            "when": "midnight"
        },
    },
    "root": {
        "handlers": ["fileHandler"],
        "level": "DEBUG",
    },
}


def main():
    os.makedirs(".data", exist_ok=True)
    status, extra = run_daemon(
        pid_filename=".data/foo.pid",
        stdout_filename=".data/out.txt",
        stderr_filename=".data/err.txt",
        daemon_entry="daemon_impl:main",
        logging_config=LOG_CONFIG,
        daemon_args=dict(foo=1, bar=2)
    )
    if status == DaemonRunStatus.LAUNCHED:
        print(f"Daemon launched, pid = {extra}")
    else:
        print(f"Unable to launch daemon, status={status}, extra={extra}")


if __name__ == '__main__':
    main()
