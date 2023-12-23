# Index
* [create daemon](#creae-daemon)
    * [pid file](#pid-file)
    * [daemon entry](#daemon-entry)
    * [daemon args](#daemon-args)
    * [I/O redirect](#io-redirect)
    * [logging](#logging)
    * [crash recovery](#crash-recovery)
* [create workers](#create-workers)
    * [worker pid file](#worker-pid-file)
    * [worker entry](#worker-entry)
    * [worker name](#worker-name)
    * [worker I/O redirect](#worker-io-redirect)
    * [worker logging](#worker-logging)
    * [worker args](#worker-args)
* [Other APIs](#other-apis)
    * [quit_requested](#quit_requested)
    * [sleep](#sleep)

# creae daemon
To create a daemon, you may call `start_daemon`. You can look at example at [here](https://github.com/stonezhong/pyappkit/blob/main/examples/daemon/test1.py)

## pid file
When you create a daemon, you need to pass a parameter `pid_filename`, it specifies a filename which the pid of the guardian process will be saved to.
* When daemon starts, it will create this pid file
* When daemon stops, the pid file will be deleted
* If the pid file already exist when you call `start_daemon`, the API will fail with `DaemonRunStatus.ALREADY_RUNNING`

## daemon entry
When you create a daemon, you need to pass a parameter `daemon_entry`, it specifies a then entry function name for the daemon. The format of this string is, `module_name:function_name`, for eample, if entry is `test1:daemon_entry`, it means the function is located at module `test1` with name `daemon_entry`

## daemon args
When you create a daemon, you need to pass a parameter `daemon_args`, it should be a `dict`, by default, it is an empty `dict`. The content of `daemon_args` will be passed to daemon entry as `**kwargs`. For example, if `daemon_args` is `{"x": 1, "y": 2}`, then the daemon entry signature shuold be like below:
```python
def daemon_entry(*, x:int, y:int):
    pass
```

## I/O redirect
Daemon is detached from the calling proces's console. So you may want to redirect the I/O if you want to see the print outout from your daemon. You can specify parameter `stdout_filename`, `stderr_filename`, which `stdout` and `stderr` will be redirected to.

By default, both of these parameters are set to `/dev/null`

A daemon should not even tried to read from stdin, stdin is already redirected to `/dev/null`

## logging
If you pass non-None `logging_config` parameter to `start_daemon` API, your daemon's logging will be initialized using it.

## crash recovery
If you pass non-None `restart_interval` parameter to `start_daemon` API, it will try to restart executor if executor exit with non-zero exitcode, `restart_interval` controls how long to wait before restart.

# create workers
You can call `start_workers` to create a bunch of workers. Each worker is a indepedent child process. You can look at example at [here](https://github.com/stonezhong/pyappkit/blob/main/examples/daemon/test2.py)

## worker pid file
Each worker has a pid file, you can specify it in `WorkerStartInfo.pid_filename`.
* When worker starts, it will create this pid file
* When worker stops, the pid file will be deleted

## worker entry
Each worker has an entry function. You specify it in `WorkerStartInfo.entry`. The format of this string is, `module_name:function_name`, for eample, if entry is `test1:worker_entry`, it means the function is located at module `test1` with name `worker_entry`

## worker name
A unique name attached to the worker.

## worker I/O redirect
Worker is detached from the console. So you may want to redirect the I/O if you want to see the print outout from your worker. You can specify parameter `WorkerStartInfo.stdout_filename`, `WorkerStartInfo.stderr_filename`, which worker's `stdout` and `stderr` will be redirected to.

By default, both of these parameters are set to `/dev/null`

A worker should not even tried to read from stdin, stdin is already redirected to `/dev/null`

## worker logging
If you pass non-None `WorkerStartInfo.logging_config` parameter to `start_workers` API, the worker's logging will be initialized using it.

## worker args
The content you passed to `WorkerStartInfo.args` will be passed to your worker entry function as `**kwargs`. For example, if `args` is `{"x": 1, "y": 2}`, then the worker entry signature shuold be like below:
```python
def worker_entry(*, x:int, y:int):
    pass
```

# Other APIs
## quit_requested
```python
def quit_requested()->bool:
    ...
```
It returns if the current guardian/executor/worker has been asked to quit or not.

## sleep
```python
def sleep(seconds:float, step_seconds:float=1):
    ...
```
It sleeps, but will retrun if quit_requested for current process becomes true.
