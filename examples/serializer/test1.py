#!/usr/bin/env python
# -*- coding: UTF-8 -*-

LOG_CONFIG = {
    "version": 1,
    "disable_existing_loggers": True,
    "formatters": {
        "standard": {
            "format": "%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s"
        }
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "DEBUG",
            "formatter": "standard",
            "stream": "ext://sys.stdout"
        },
    },
    "root": {
        "handlers": ["console"],
        "level": "DEBUG",
    },
}
import logging.config
from typing import Type
logging.config.dictConfig(LOG_CONFIG)

import logging
logger = logging.getLogger(__name__)


def main():
    logger.info("Blah...")

if __name__ == '__main__':
    main()
