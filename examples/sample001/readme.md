```
In this example:

    status, extra = run_daemon(
        pid_filename=".data/foo.pid",
        stdout_filename=".data/out.txt",
        stderr_filename=".data/err.txt",
        daemon_entry="daemon_impl:main",
        logging_config=LOG_CONFIG,
        daemon_args=dict(foo=1, bar=2),
        restart_interval=timedelta(seconds=10)
    )
```
* In this example:
    * It launches a daemon in the background, the daemon is detached from the login session that launches it.
    * Once the daemon is launched, it put the daemon's pid in pid_filename
        * If the pid file already exist, launching the daemon will fail, it is likely the daemon is already running or quit uncleanly.
    * The daemon's stdout will be directed to file stdout_filename
    * The daemon's stderr will be directed to file stderr_filename
    * The daemon's stdin will be /dev/null, so the daemon should not try to read from stdin
    * The daemon_entry specifies the entry function name
        * The daemon_entry should have signature `def function_name(daemon_args: Dict, quit_requested: Callable[[], bool]) -> None`
    * logging_config is a `dict`, it is used for the logging
    * You can pass daemon_args, which is a `dict`, the daemon_args will be passed to daemon_entry function as `daemon_args`
    * The daemon is a `guardian` process, it will launch an executor to run `daemon_entry`
    * If the `guardian` got kill signal (SIGTERM), it will send SIGTERM to the current `executor` it launched
        * `guardian` will quit in 3 cases
            * (1) `executor` finished successfully (`daemon_entry` has no exception)
            * (2) `restart_interval` is None -- which means no retry upon failure is needed, and `executor` failed
            * (3) `guardian` got SIGTERM -- since it has sent a SIGTERM to executor, it will wait for the executor to finish and then quit
    * If the executor got kill signal (SIGTERM), quit_requested() will retrun True
        * The `daemon_entry` and down the chain shoud constantly pull quit_requested() and gracefully cleanup and quit if quit_requestd() returns True.
    * If the code in `daemon_entry` throws exception, the executor will exit with non-zero, and thus the guardian will try to re-launched it if `restart_interval` is not None, it will wait `restart_interval` time and try to spawn a new executor.
