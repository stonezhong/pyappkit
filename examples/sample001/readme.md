```
In this example:

    status, extra = run_daemon(
        pid_filename=".data/foo.pid",
        stdout_filename=".data/out.txt",
        stderr_filename=".data/err.txt",
        daemon_entry="daemon_impl:main",
        logging_config=LOG_CONFIG,
        daemon_args=dict(foo=1, bar=2)
    )
```
* In this example:
    * It launches a daemon in the background, the daemon is detached from the loigin session that launches it.
    * Once the daemon is launched, it put the daemon's pid in pid_filename
        * If the pid file already exist, launching the daemon will fail, it is likely the daemon is already running.
    * The daemon's stdout will be directed to file stdout_filename
    * The daemon's stderr will be directed to file stderr_filename
    * The daemon's stdin will be /dev/null, so the daemon should not try to read from stdin
    * The daemon_entry specifies the entry function name
        * The daemon_entry should have signature `def function_name(daemon_args: Dict, quit_requested: Callable[[], bool]) -> None`
    * logging_config is a `dict`, it is used for the logging
    * You can pass daemon_args, which is a `dict`, the daemon_args will be passed to daemon_entry function as `daemon_args`
    * If the daemon process got kill signal (SIGTERM), quit_requested() will retrun True
        * The `daemon_entry` and down the chain shoud constantly pull quit_requested() and gracefully cleanup and quit if quit_requestd() returns True.
    * If `daemon _entry` throws exception, the daemon will be terminated and you should check stderr or log file.
