import datetime
import json
from importlib import import_module
import os
import signal
from time import sleep

import luigi
import pidfile


seconds = 1
minutes = 60 * seconds
hours = 60 * minutes


# http://stackoverflow.com/a/2680060/1118576
dthandler = lambda obj: (obj.strftime('%Y-%m-%d')
                         if isinstance(obj, datetime.datetime)
                         or isinstance(obj, datetime.date)
                         else None)


def fullname(o):
    """Tuple of module name and class name from object"""
    return o.__class__.__module__, o.__class__.__name__


def get_class(module_name, class_name):
    """Class from module name and class name"""
    module = import_module(module_name)
    return getattr(module, class_name)


def kill_from_pid_file(pid_file, sig):
    """Signal a process given its PID file

    Sends signal ``sig`` to the process ID found in the PID file. Does not
    raise an error if the file does not exist, a process ID could not be
    found in the file, or the process was not found.

    :param pid_file: path to PID file
    :type pid_file: str
    :param sig: signal to send to process
    :type sig: signal constant from :mod:`signal` module
               (i.e. ``signal.SIGTERM``)
    """
    try:
        with open(pid_file, 'r') as pid_f:
            pid = int(pid_f.read().strip())
        os.kill(pid, sig)
    except (IOError, OSError, ValueError):
        pass


class SingleWaitingLockPidFile(object):
    """Locks a PID file, sending ``SIGHUP`` to anyone who's already waiting.

    >>> with SingleWaitingLockPidFile('aoeu.pid', 60.):
    ...     print('test')

    :param pidfilepath: path to PID file to lock
    :type pidfilepath: str
    :param timeout_sec: how long to wait to lock the PID file (in seconds)
    :type timeout_sec: int
    """

    def __init__(self, pidfilepath, timeout_sec):
        self.pidfilepath = pidfilepath
        self.timeout_sec = timeout_sec

    def __enter__(self):
        # Get lock on the "waiting" pid file.
        wait_pidfile_path = self.pidfilepath + '-waiting'
        self.wait_pidfile = pidfile.PidFile(wait_pidfile_path)
        while True:
            try:
                self.wait_pidfile.__enter__()
                break
            except SystemExit:
                # Terminate a prior waiting process.
                kill_from_pid_file(wait_pidfile_path, signal.SIGHUP)
        # Get the main pid file lock.
        self.pidfile = pidfile.PidFile(self.pidfilepath)
        t_start = datetime.datetime.now()
        t_elapsed = lambda: (datetime.datetime.now() - t_start).total_seconds()
        while True:
            if t_elapsed() > self.timeout_sec:
                raise RuntimeError('Timed out trying to get PID file lock')
            try:
                self.pidfile.__enter__()
                break
            except SystemExit:
                sleep(2)
        try:
            self.wait_pidfile.__exit__(None, None, None)
        except:
            pass

    def __exit__(self, *args):
        try:
            self.pidfile.__exit__(None, None, None)
            self.wait_pidfile.__exit__(None, None, None)
        except:
            pass


def get_luigi_params(task):
    """
    >>> import luigi
    >>> class TaskA(luigi.Task):
    ...     p1 = luigi.Parameter(default='foo')
    ...     p2 = luigi.Parameter(default='bar')
    ...     v1 = 'aoeu'
    >>> get_luigi_params(TaskA()) == {'p1': 'foo', 'p2': 'bar'}
    True
    """
    result = dict()
    for attr in dir(task):
        if attr == 'pool':
            continue
        param_type = getattr(task.__class__, attr, None)
        if isinstance(param_type, luigi.Parameter):
            result[attr] = getattr(task, attr)
    return result


# It would be nice to read this from a config file, but ConfigParser doesn't
# allow escaping of percent signs in any reasonable way.
default_log_format = '%(asctime)s - %(levelname)s - %(name)s - %(message)s'


def get_all_tasks(task, include_obj=False):
    deps = task.deps()
    start_to_close = getattr(task, 'swf_start_to_close_timeout', None)
    if start_to_close is None:
        start_to_close = 'NONE'
    else:
        start_to_close = int(start_to_close)
    schedule_to_start = getattr(task, 'swf_schedule_to_start_timeout', None)
    if schedule_to_start is None:
        schedule_to_start = int(5 * minutes)
    else:
        schedule_to_start = int(schedule_to_start)
    heartbeat = getattr(task, 'swf_heartbeat_timeout', None)
    if heartbeat is None:
        heartbeat = 'NONE'
    else:
        heartbeat = int(heartbeat)
    schedule_to_close = 'NONE'
    tasks = {
        task.task_id: {
            'class': fullname(task),
            'task_family': task.task_family,
            'deps': [d.task_id for d in deps],
            'task_list': getattr(task, 'swf_task_list', 'default'),
            'params': json.dumps(get_luigi_params(task), default=dthandler),
            'retries': getattr(task, 'swf_retries', 0),
            'heartbeat_timeout': heartbeat,
            'start_to_close_timeout': start_to_close,
            'schedule_to_start_timeout': schedule_to_start,
            'schedule_to_close_timeout': schedule_to_close,
            'is_wrapper': isinstance(task, luigi.WrapperTask),
            'running_mutex': getattr(task, 'swf_running_mutex', None),
        }
    }
    if include_obj:
        tasks[task.task_id]['task'] = task
    for dep in deps:
        tasks.update(get_all_tasks(dep))
    return tasks


def dt_from_iso(iso):
    return datetime.date(*map(int, iso.split('-')))


if __name__ == "__main__":
    import doctest
    doctest.testmod()
