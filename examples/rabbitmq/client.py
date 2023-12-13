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
    "loggers": {
        "pika": {
            "handlers": ["console"],
            "level": "FATAL",
            "proagate": False
        }
    }
}
import logging.config
logging.config.dictConfig(LOG_CONFIG)

import logging
logger = logging.getLogger(__name__)


import time
import signal
import pika
from pyappkit.rabbitmq import MQClient, MQProcessor, Serializer, MessageEnvelope, MessageDebugInfo
from datetime import datetime, timedelta
from models import Message, MySerializer



def main():
    serializer = MySerializer()

    mq_client = MQClient(
        username="stonezhong",
        password="foobar",
        hostname="localhost"
    )

    try:
        connection, channel = mq_client.get_channel()
        with connection:
            with channel:
                while True:
                    line = input("? ")
                    x, y = [int(i) for i in line.split(" ")]
                    message = Message(x=x, y=y)
                    mq_client.send_message(queue_name="normal", channel=channel, message=message, serialize=serializer)

    except KeyboardInterrupt:
        print("Bye!")


if __name__ == '__main__':
    main()
