from typing import Optional, Any, Dict
import os
import json
import yaml
import jinja2
from copy import copy

ENV_HOME = os.environ["ENV_HOME"]

def get_app_config_from_file(caller_filename:str) -> Optional["ApplicationConfig"]:
    app_home = os.path.join(ENV_HOME, "apps/")
    if not caller_filename.startswith(app_home):
        return None
    app_name = caller_filename[len(app_home):].split("/")[0]
    return ApplicationConfig(app_name)

class ApplicationConfig:
    name:str
    stage:str   # beta or prod
    cfg_dir:str
    log_dir:str
    data_dir:str
    pid_dir:str
    app_dir:str
    daemon_status_dir:str
    _ctx:Dict[str, str]

    def __init__(self, name:str):
        self.name               = name
        self.cfg_dir            = os.path.join(ENV_HOME, "configs", self.name)
        self.log_dir            = os.path.join(ENV_HOME, "logs", self.name)
        self.data_dir           = os.path.join(ENV_HOME, "data", self.name)
        self.pid_dir            = os.path.join(ENV_HOME, "pids", self.name)
        self.daemon_status_dir  = os.path.join(ENV_HOME, "daemon_status", self.name)
        self.app_dir            = os.path.join(ENV_HOME, "apps", self.name, "current")

        self._ctx = {
            "app_name": self.name,
            "cfg_dir": self.cfg_dir,
            "log_dir": self.log_dir,
            "data_dir": self.data_dir,
            "pid_dir": self.pid_dir,
            "app_dir": self.app_dir,
            "daemon_status_dir": self.daemon_status_dir
        }
        # you always have this file if you are using mordor to deploy your app
        self.stage = self.get_json(os.path.join(self.cfg_dir, "_deployment.json"))["app"]["stage"]
        self._ctx.update({
            "stage": self.stage
        })

    def __repr__(self):
        return f"ApplicationConfig(name=\"{self.name}\", stage=\"{self.stage}\")"

    def get_config_filename(self, name:str)->str:
        return os.path.join(self.cfg_dir, name)

    def get_log_filename(self, name:str)->str:
        return os.path.join(self.log_dir, name)

    def get_data_filename(self, name:str)->str:
        return os.path.join(self.data_dir, name)

    def get_pid_filename(self, name:str)->str:
        return os.path.join(self.data_dir, name)

    def get_json(self, filename:str, context:dict={})->Optional[dict]:
        if not os.path.isfile(filename):
            return None
        content = self.load_template(filename, context=context)
        return json.loads(content)

    def get_yaml(self, filename:str, context:dict={})->Optional[dict]:
        if not os.path.isfile(filename):
            return None
        content = self.load_template(filename, context=context)
        return yaml.safe_load(content)

    def load_template(self, filename:str, context:dict={}):
        if not os.path.isfile(filename):
            return None

        environment = jinja2.Environment()
        ctx = copy(self._ctx)
        ctx.update(context)
        with open(filename, "rt") as f:
            template = environment.from_string(f.read())
            return template.render(**ctx)

    def get_manifest(self) -> "Manifest":
        filename = os.path.join(self.app_dir, "manifest.yaml")
        if os.path.isfile(filename):
            return Manifest(self.get_yaml(filename))
        filename = os.path.join(self.app_dir, "manifest.json")
        if os.path.isfile(filename):
            return Manifest(self.get_json(filename))
        return None

class Manifest:
    version:str
    def __init__(self, payload:dict):
        self.version = payload["version"]
