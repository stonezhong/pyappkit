#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from config import LOG_CONFIG
import logging.config
logging.config.dictConfig(LOG_CONFIG)

import logging
logger = logging.getLogger(__name__)

from pyappkit.rabbitmq import MQClient

def main():
    mq_client = MQClient(
        username="stonezhong",
        password="foobar",
        hostname="localhost"
    )

    # create 3 message queues, enabel delayed exchange for retry queue
    mq_client.create_queue("normal")
    mq_client.create_queue("retry")
    mq_client.create_queue("dead")
    mq_client.enable_delayed_exchange("retry")

if __name__ == '__main__':
    main()
