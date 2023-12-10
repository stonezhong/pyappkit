import logging
logger = logging.getLogger(__name__)

from datetime import timedelta
from pyappkit import Executable

class MyDaemon(Executable):
    def run(self, *, foo:int, bar:int)->None:
        logger.info(f"foo={foo}, bar={bar}")
        while True:
            print("I am running, ...")
            logger.info("I am running, ...")
            self.sleep(timedelta(seconds=10))
            if self.quit_requested():
                print("I am done!")
                break
