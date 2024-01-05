import logging
logger = logging.getLogger(__name__)

from datetime import datetime, timedelta
from pyappkit  import sleep, quit_requested, mordor_start_workers, MordorWorkerStartInfo

def daemon_entry(*, app_name:str, daemon_name:str, x:int, y:int):
    # app_name: the mordor application name
    # daemon_name: the daemon name
    logger.debug(f"daemon_entry Enter: app_name={app_name}, daemon_name={daemon_name}, x={x}, y={y}")
    mordor_start_workers(
        app_name,
        daemon_name,
        [
            MordorWorkerStartInfo(
                entry = "demo1:worker_entry",
                name="foo",
                args={"color": "red"},
            ),
            MordorWorkerStartInfo(
                entry = "demo1:worker_entry",
                name="bar",
                args={"color": "green"},
            ),
        ],
        check_interval=timedelta(minutes=1),
        restart_interval=timedelta(minutes=5)
    )
    logger.debug(f"daemon_entry Exit")


def worker_entry(*, app_name:str, daemon_name:str, worker_name:str, color:str):
    logger.debug(f"worker_entry: enter, app_name={app_name}, daemon_name={daemon_name}, worker_name={worker_name}, color={color}")
    while not quit_requested():
        logger.debug("I am running")
        sleep(5)
    logger.debug("demo quits")
    logger.debug(f"worker_entry: exit")
