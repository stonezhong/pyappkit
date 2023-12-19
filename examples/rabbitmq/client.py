#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from config import LOG_CONFIG
import logging.config
logging.config.dictConfig(LOG_CONFIG)

import logging
logger = logging.getLogger(__name__)


from pyappkit.rabbitmq import MQClient
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
