import logging
logger = logging.getLogger(__name__)

#########################################################################################################
# Integration with mordor application
#########################################################################################################
from typing import Optional, Tuple, Dict, List
import argparse
import os
from copy import deepcopy, copy
from datetime import timedelta
from enum import Enum
import sys
import signal
from pydantic import BaseModel, computed_field

from pyappkit.app_config import get_app_config_from_file, ApplicationConfig
from .daemon import start_daemon, DaemonRunStatus
from .worker import WorkerStartInfo, start_workers
from .tools  import get_method, read_int_file

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
            "filename": None,    # will override
            "interval": 1,
            "when": "midnight"
        }
    },
    "root": {
        "handlers": ["fileHandler"],
        "level": "DEBUG",
    }
}

####################################################################
# Models
####################################################################
class DaemonConfig(BaseModel):
    description: str=""
    args: dict={}
    restart_interval_seconds: Optional[float] = None

    @computed_field
    @property
    def restart_interval(self)->Optional[timedelta]:
        return None if self.restart_interval_seconds is None else timedelta(seconds=self.restart_interval_seconds)

class DaemonInfo:
    app_config: ApplicationConfig
    daemon_name: str
    pid_filename: str
    stdout_filename: str
    stderr_filename: str
    log_filename: str

    def __init__(self, *, app_config, daemon_name:str):
        self.daemon_name        = daemon_name
        self.app_config         = app_config
        self.pid_filename       = os.path.join(app_config.pid_dir, f"{daemon_name}.pid")
        self.stdout_filename    = os.path.join(app_config.log_dir, f"{daemon_name}-out.txt")
        self.stderr_filename    = os.path.join(app_config.log_dir, f"{daemon_name}-err.txt")
        self.log_filename       = os.path.join(app_config.log_dir, f"{daemon_name}.log")
        self.debug_filename    =  os.path.join(app_config.daemon_status_dir, f"{daemon_name}.json")

        os.makedirs(os.path.join(app_config.pid_dir, daemon_name), exist_ok=True)
        os.makedirs(os.path.join(app_config.log_dir, daemon_name), exist_ok=True)
        os.makedirs(app_config.daemon_status_dir, exist_ok=True)

    def get_worker_pid_filename(self, worker_name:str)->str:
        return os.path.join(self.app_config.pid_dir, self.daemon_name, f"{worker_name}.pid")

    def get_worker_log_filename(self, worker_name:str)->str:
        return os.path.join(self.app_config.log_dir, self.daemon_name, f"{worker_name}.log")

    def get_worker_stdout_filename(self, worker_name:str)->str:
        return os.path.join(self.app_config.log_dir, self.daemon_name, f"{worker_name}-out.txt")

    def get_worker_stderr_filename(self, worker_name:str)->str:
        return os.path.join(self.app_config.log_dir, self.daemon_name, f"{worker_name}-err.txt")

    @property
    def pid(self)->Optional[int]:
        return read_int_file(self.pid_filename)

####################################################################
# Purpose: get daemon config in raw format
#          return None if the file is not found
####################################################################
def get_daemon_config_configs_raw(app_config:ApplicationConfig, context:dict={})->Optional[dict]:
    daemons_config_filename = os.path.join(app_config.app_dir, "daemons.yaml")
    if not os.path.isfile(daemons_config_filename):
        return None
    return app_config.get_yaml(daemons_config_filename, context=context)

####################################################################
# Purpose: return names for all daemons
####################################################################
def get_daemon_names(app_config:ApplicationConfig)->List[str]:
    daemon_configs_raw = get_daemon_config_configs_raw(app_config)
    return list(daemon_configs_raw.keys())

####################################################################
# Purpose: Get config for a given daemon
####################################################################
def get_daemon_config(app_config:ApplicationConfig, daemon_name:str, context:dict={})->Optional[DaemonConfig]:
    context = {
        "daemon_name": daemon_name
    }
    raw_configs = get_daemon_config_configs_raw(app_config, context)
    if raw_configs is None:
        return None
    if daemon_name not in raw_configs:
        return None
    return DaemonConfig.model_validate(raw_configs[daemon_name])


########################
#### cli for daemons ###
########################
def cli_daemon():
    parser = argparse.ArgumentParser(
        description='Daemon controller.'
    )
    parser.add_argument(
        "action", type=str, help="Specify action",
        choices=['list', 'start', 'stop'],
        nargs=1
    )
    parser.add_argument(
        "-dn", "--daemon-name", type=str, default=None, required=False,
        help="daemon name"
    )
    args = parser.parse_args()
    action = args.action[0]
    if action == "list":
        cli_daemon_list(args)
    elif action == "start":
        cli_daemon_start(args)
    elif action == "stop":
        cli_daemon_stop(args)

def cli_daemon_list(args:argparse.Namespace):
    app_config = get_app_config_from_file(os.getcwd())
    daemon_names = get_daemon_names(app_config)
    print("----------------------+--------+-----------------")
    print("name                  | status | pid             ")
    print("----------------------+--------+-----------------")
    for daemon_name in daemon_names:
        daemon_info = DaemonInfo(app_config=app_config, daemon_name=daemon_name)
        daemon_pid = daemon_info.pid
        is_running = "no" if daemon_pid is None else "yes"
        daemon_pid = "" if daemon_pid is None else daemon_pid
        print(f"{daemon_name:21} | {is_running:6} | {daemon_pid}")
    print("----------------------+--------+-----------------")

def cli_daemon_start(args:argparse.Namespace):
    app_config = get_app_config_from_file(os.getcwd())
    sys.path.insert(0, app_config.app_dir)

    daemon_name = args.daemon_name
    if daemon_name is None:
        print(f"Please specify daemon name using -dn or --daemon-name")
        return

    daemon_config = get_daemon_config(app_config, daemon_name)
    if daemon_config is None:
        print(f"Daemon {daemon_name} is not found")
        return

    daemon_info = DaemonInfo(app_config=app_config, daemon_name=daemon_name)
    logging_config = deepcopy(LOG_CONFIG)
    logging_config["handlers"]["fileHandler"]["filename"] = daemon_info.log_filename

    effective_daemon_args = copy(daemon_config.args)
    effective_daemon_args.update({"app_name": app_config.name, "daemon_name": daemon_name})

    status, pid = start_daemon(
        pid_filename        = daemon_info.pid_filename,
        daemon_entry        = f"{daemon_name}:daemon_entry",
        daemon_args         = effective_daemon_args,
        stdout_filename     = daemon_info.stdout_filename,
        stderr_filename     = daemon_info.stderr_filename,
        logging_config      = logging_config,
        restart_interval    = daemon_config.restart_interval
    )
    if status == DaemonRunStatus.LAUNCHED:
        print(f"Daemon {daemon_name} started, pid = {pid}")
    elif status == DaemonRunStatus.ALREADY_RUNNING:
        print(f"Daemon {daemon_name} is arelady running, pid = {pid}")
    elif status == DaemonRunStatus.LAUNCH_FAILED:
        print(f"Daemon {daemon_name} failed to start")


def cli_daemon_stop(args:argparse.Namespace):
    app_config = get_app_config_from_file(os.getcwd())
    sys.path.insert(0, app_config.app_dir)

    daemon_name = args.daemon_name
    if daemon_name is None:
        print(f"Please specify daemon name using -dn or --daemon-name")
        return

    daemon_config = get_daemon_config(app_config, daemon_name)
    if daemon_config is None:
        print(f"Daemon {daemon_name} is not found")
        return

    daemon_info = DaemonInfo(app_config=app_config, daemon_name=daemon_name)
    pid = daemon_info.pid
    if pid is None:
        print(f"Daemon {daemon_name} is not running")
        return

    os.kill(pid, signal.SIGTERM)
    print(f"Request daemon {daemon_name} to stop")

class MordorWorkerStartInfo:
    entry: str
    name: str
    args: dict

    def __init__(
        self,
        *,
        entry:str,
        name:str,
        args:dict=dict()
    ):
        self.entry = entry
        self.name = name
        self.args = args

def mordor_start_workers(
    app_name:str,
    daemon_name:str,
    mordor_worker_start_infos: List[MordorWorkerStartInfo],
    *,
    check_interval:timedelta=timedelta(minutes=1),
    restart_interval:timedelta=timedelta(minutes=5)
)->bool:
    app_config = ApplicationConfig(app_name)
    daemon_info = DaemonInfo(app_config=app_config, daemon_name=daemon_name)
    worker_start_infos:List[WorkerStartInfo] = []
    for mordor_worker_start_info in mordor_worker_start_infos:
        effective_args = copy(mordor_worker_start_info.args)
        effective_args.update({
            "app_name": app_name,
            "daemon_name": daemon_name,
            "worker_name": mordor_worker_start_info.name
        })
        logging_config = deepcopy(LOG_CONFIG)
        logging_config["handlers"]["fileHandler"]["filename"] = daemon_info.get_worker_log_filename(mordor_worker_start_info.name)
        worker_start_info = WorkerStartInfo(
            pid_filename = daemon_info.get_worker_pid_filename(mordor_worker_start_info.name),
            entry = mordor_worker_start_info.entry,
            name = mordor_worker_start_info.name,
            args = effective_args,
            logging_config = logging_config,
            stdout_filename=daemon_info.get_worker_stdout_filename(mordor_worker_start_info.name),
            stderr_filename=daemon_info.get_worker_stderr_filename(mordor_worker_start_info.name),
        )
        worker_start_infos.append(worker_start_info)

    return start_workers(
        worker_start_infos,
        debug_filename=daemon_info.debug_filename,
        check_interval=check_interval,
        restart_interval=restart_interval
    )
