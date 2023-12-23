from typing import Optional, IO
import importlib
import os
from enum import Enum
from multiprocessing import Value
import signal
import sys
import time

DT_FORMAT = "%Y-%m-%d %H:%M:%S.%f"

class ProcessRole(Enum):
    GUARDIAN = 1
    EXECUTOR = 2
    WORKER   = 3

def read_int_file(filename: str) -> Optional[int]:
    if not os.path.isfile(filename):
        return None
    with open(filename, "rt") as f:
        return int(f.read())

class SigTermHandler:
    quit_requested      : bool                      # have we ever received SIGTERM?
    executor_pid        : Optional[int]             # execuror pid
    guardian_pid        : Optional[int]             # guardian pid
    role                : Optional[ProcessRole]     # the role of current process
    worker_controller   : Optional[Value]           # lives in executor, controls if workers should quit
    guardian_killed     : bool                      # have we sent SIGTERM to guardian?
    executor_killed     : bool                      # have we sent SIGTERM to executor?

    def __init__(self):
        self.quit_requested = False
        self.executor_pid = None
        self.guardian_pid = None
        self.role = None
        self.worker_controller = None
        self.guardian_killed = False
        self.executor_killed = False

    def handle(self, signal_number, frame):
        self.quit_requested = True

        if self.role == ProcessRole.GUARDIAN:
            if not self.executor_killed:
                safe_kill(self.executor_pid)
                self.executor_killed = True
            return

        if self.role == ProcessRole.EXECUTOR:
            if self.worker_controller is not None:
                self.worker_controller.value = True
            if not self.guardian_killed:
                safe_kill(self.guardian_pid)
                self.guardian_killed = True
            return

        if self.role == ProcessRole.WORKER:
            # TODO
            pass

    def register(self):
        signal.signal(signal.SIGTERM, lambda signal_number, frame: self.handle(signal_number, frame))

SIG_TERM_HANDLER = SigTermHandler()

def safe_kill(pid:Optional[int]):
    try:
        if pid is not None:
            os.kill(pid, signal.SIGTERM)
    except OSError:
        pass

def redirect_io(stdout_filename:str, stderr_filename:str)->bool:
    out_f = None
    err_f = None
    in_f = None
    try:
        if stdout_filename == stderr_filename:
            # use the same file for stdout and stderr
            out_f = err_f = open(stdout_filename, "ab")
        else:
            out_f = open(stdout_filename, "ab")
            err_f = open(stderr_filename, "ab")
        in_f = open(os.devnull)

        stdout_fn = sys.stdout.fileno()
        stderr_fn = sys.stderr.fileno()
        stdin_fn = sys.stdin.fileno()
        os.close(stdout_fn)
        os.close(stderr_fn)
        os.close(stdin_fn)
        os.dup2(out_f.fileno(), stdout_fn)
        os.dup2(err_f.fileno(), stderr_fn)
        os.dup2(in_f.fileno(), stdin_fn)
        os.setsid()
        return True
    except:
        return False
    finally:
        safe_close_io(in_f, out_f, err_f)


def safe_close(f: IO):
    try:
        if f is not None:
            f.close()
    except OSError:
        pass

def safe_close_io(in_f:IO, out_f: IO, err_f: IO):
    if out_f is err_f:
        safe_close(out_f)
    else:
        safe_close(out_f)
        safe_close(err_f)
    safe_close(in_f)

def safe_remove(filename: str):
    try:
        os.remove(filename)
    except OSError:
        pass

def get_method(method_name:str):
    module_name, entry_name = method_name.split(":")
    module = importlib.import_module(module_name)
    entry = getattr(module, entry_name)
    return entry

def quit_requested():
    # works for worker, guardian and executor
    if SIG_TERM_HANDLER.role == ProcessRole.WORKER:
        return SIG_TERM_HANDLER.worker_controller.value
    return SIG_TERM_HANDLER.quit_requested

def sleep(seconds:float, step_seconds:float=1):
    begin_time = time.time()
    while True:
        if quit_requested():
            return
        if time.time() - begin_time >= seconds:
            return
        time.sleep(step_seconds)

