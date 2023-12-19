#!/usr/bin/env python
# -*- coding: UTF-8 -*-

from config import LOG_CONFIG
import logging.config
logging.config.dictConfig(LOG_CONFIG)

import logging
logger = logging.getLogger(__name__)

import argparse
import signal
from pyappkit.rabbitmq import MQClient, MQProcessor, MessageEnvelope
from datetime import timedelta
from models import MySerializer

ctx = {
    "quit": False
}

def __request_shutdown(signal_umber, frame):
    _, _ = signal_umber, frame
    ctx['quit'] = True

def quit_requested():
    return ctx["quit"]

class MyProcessor(MQProcessor):
    def handle_message(self, *, is_retry:bool, message_envelope:MessageEnvelope)->None:
        message = message_envelope.message
        logger.info(f"got message: {message}")
        if message.x == 10:
            raise Exception("Oops")


def main():
    parser = argparse.ArgumentParser(
        description='Mordor deployment tool for python.'
    )
    parser.add_argument(
        "-ie", "--is-error", action="store_true", help="is retry queue"
    )
    args = parser.parse_args()


    signal.signal(signal.SIGTERM, __request_shutdown)  # regular kill command
    serializer = MySerializer()

    mq_client = MQClient(
        username="stonezhong",
        password="foobar",
        hostname="localhost"
    )

    processor = MyProcessor(
        mq_client=mq_client,
        serializer=serializer,
        queue_name="normal",
        retry_queue_name="retry",
        failed_queue_name="dead",
        retry_count=2,
        quit_requested=quit_requested,
        inactivity_timeout=timedelta(seconds=5),
        retry_delay_seconds=300  # message send to the retry queue will wait after 5 minutes before being processed
    )

    try:
        processor.process_messages(use_retry_queue=args.is_error)
    except KeyboardInterrupt:
        print("Bye!")



if __name__ == '__main__':
    main()
