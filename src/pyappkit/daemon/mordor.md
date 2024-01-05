# Index
* [purpose](#purpose)
* [Create daemons for mordor applications](#create-daemons-for-mordor-applications)

# Purpose

If you deploy your application using [mordor](https://github.com/stonezhong/mordor), using mordor integration make it easier to create daemon

# Create daemons for mordor applications

First, you need to create a config file, and name it `daemons.yaml` in your source code root directory.

Here is an example:
```yaml
demo1:
  args:
    x: 1
    y: 2
  restart_interval_seconds: null
```

Here are some sections in this yaml file:
* each daemon has a section, name is daemon mame. So in the example, we defined one daemon, the name of the daemon is `demo1`
* You can pass args to each daemon via `args` parameter, it is a dict
* `restart_interval_seconds`: if missing or null, when daemon crash, it won't try to recover. Otherwise, it will try to recover after the time specified here.
* for each daemon, you need a module with the daemon name, for example here, you need a module `demo1.py` for daemon `demo1`
    * The daemon module MUST have an function called `daemon_entry`, it is the entry of the daemon. It must have the following signature:
    ```python
    # note, here the argument x and y are defined in the yaml file
    def daemon_entry(*, x:int, y:int):
        ...
    ```

# daemon behavior
* The pid file is at `$ENV_HOME/pids/{app_name}/{daemon_name}.pid`
* The log file is at `$ENV_HOME/logs/{app_name}/{daemon_name}.log`
* Daemon's stdout is at `$ENV_HOME/logs/{app_name}/{daemon_name}-out.txt`
* Daemon's stderr is at `$ENV_HOME/logs/{app_name}/{daemon_name}-err.txt`
* You can use command `daemon list` to list all daemons for current mordor application
* You can use command `daemon start -dn {daemon_name}` to start a daemon
* You can use command `daemon stop -dn {daemon_name}` to stop a daemon
