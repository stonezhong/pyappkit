import logging
logger = logging.getLogger(__name__)

import time
from typing import Dict, Callable


def main(daemon_args: Dict, quit_requested: Callable[[], bool]) -> None:
    logger.info(daemon_args)

    while True:
        print("I am running, ...")
        logger.info("I am running, ...")
        time.sleep(1)
        if quit_requested():
            print("I am done!")
            break
