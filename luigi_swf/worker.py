import json
import logging
import os
import os.path
import signal
from subprocess import call
from time import sleep
import traceback

import boto.swf.layer2 as swf
import daemon
import luigi
import luigi.configuration

from .util import default_log_format, get_class, kill_from_pid_file, \
    SingleWaitingLockPidFile, dt_from_iso


logger = logging.getLogger(__name__)


seconds = 1.


class LuigiSwfWorker(swf.ActivityWorker):
    """Implementation of boto's SWF Activity Worker

    See :class:`WorkerServer` for daemonizing this.
    """

    def run(self, identity=None):
        """Poll for and run an activity task

        This should be run in a loop. It will poll for up to 60 seconds. After
        60 seconds, it will return without running any activity tasks. The user
        should usually not need to interact with this class directly. Instead,
        :class:`WorkerServer` can be used to run the loop.

        :return: None
        """
        activity_task = self.poll(identity=identity)
        if 'activityId' not in activity_task:
            logger.debug('LuigiSwfWorker().run(), poll timed out')
            return
        task = None
        try:
            logger.info('LuigiSwfWorker().run(), %s, processing',
                        activity_task['activityId'])
            input_task = json.loads(activity_task.get('input'))
            input_params = json.loads(input_task['params'])
            task_cls = get_class(*input_task['class'])
            task_params = task_cls.get_params()
            kwargs = dict()
            for param_name, param_cls in task_params:
                if param_name == 'pool':
                    continue
                if isinstance(param_cls, luigi.DateParameter):
                    kwargs[param_name] = dt_from_iso(input_params[param_name])
                else:
                    kwargs[param_name] = input_params[param_name]
            task = task_cls(**kwargs)
            if hasattr(task, 'register_activity_worker'):
                task.register_activity_worker(self, activity_task)
            if task.complete():
                result = 'Did not run (task.complete() returned true)'
                logger.debug('LuigiSwfWorker().run(), %s, %s', result)
                self.complete(result=result)
                return
            task.run()
            if not getattr(task, 'cancel_acked', False):
                task.on_success()
                task_completed = task.complete()
            else:
                task_completed = False
        except Exception as error:
            tb = traceback.format_exc()
            message = None
            if task is not None:
                try:
                    message = task.on_failure(error)
                except:
                    message = ('on_failure() failed: \n' +
                               traceback.format_exc())
            if message is None:
                message = '(no message)'
            logger.error('LuigiSwfWorker().run(), %s, error:\n%s',
                         activity_task['activityId'], tb)
            details = (tb + '\n\n\n' + str(error) + '\n\n\non_failure():\n' +
                       message)
            self.fail(reason=str(error)[:255], details=details[:32767])
            raise
        if task_completed:
            self.complete()
            logger.info('LuigiSwfWorker().run(), completed %s',
                        activity_task['activityId'])
        elif not getattr(task, 'cancel_acked', False):
            reason = 'complete() returned false after running'
            logger.error('LuigiSwfWorker().run(), %s, failed (%s)',
                         activity_task['activityId'], reason)
            self.fail(reason=reason)


class WorkerServer(object):
    """Decider daemon

    Daemonizes :class:`LuigiSwfWorker`. The ``SIGWINCH`` signal is used to
    shut this down lazily (after processing the current activity task or
    60-second poll) because ``SIGTERM`` kills child processes.

    :param worker_idx: worker index (instance number)
    :type worker_idx: int
    :param stdout: stream to which stdout will be written
    :type stdout: stream (such as the return value of :func:`open`)
    :param stderr: stream to which stderr will be written
    :type stderr: stream (such as the return value of :func:`open`)
    :param logfilename: file path to which the application log will be written
    :type logfilename: str
    :param loglevel: log level
    :type loglevel: log level constant from the :mod:`logging` module
                    (``logging.DEBUG``, ``logging.INFO``, ``logging.ERROR``,
                    etc.)
    :param logformat: format string of log output lines, as in the
                      :mod:`logging` module
    :type logformat: str
    :return: None
    """

    _got_term_signal = False

    def __init__(self, worker_idx, identity=None, stdout=None, stderr=None,
                 logfilename=None, loglevel=logging.INFO,
                 logformat=default_log_format, **kwargs):
        self.worker_idx = worker_idx
        config = luigi.configuration.get_config()
        if stdout is None:
            stdout_path = config.get('swfscheduler', 'worker-log-out')
            self.stdout = open(stdout_path + '-' + str(worker_idx), 'a')
        else:
            self.stdout = stdout
        if stderr is None:
            stderr_path = config.get('swfscheduler', 'worker-log-err')
            self.stderr = open(stderr_path + '-' + str(worker_idx), 'a')
        else:
            self.stderr = stderr
        if logfilename is not None:
            self.logfilename = logfilename + '-' + str(worker_idx)
        else:
            self.logfilename = config.get('swfscheduler', 'worker-log') + \
                '-' + str(worker_idx)
        self.loglevel = loglevel
        self.logformat = logformat
        if 'domain' not in kwargs:
            kwargs['domain'] = config.get('swfscheduler', 'domain')
        if 'task_list' not in kwargs:
            tlist = config.get('swfscheduler', 'worker-task-list', 'default')
            kwargs['task_list'] = tlist
        if 'aws_access_key_id' not in kwargs or \
                'aws_secret_access_key' not in kwargs:
            access_key = config.get('swfscheduler', 'aws_access_key_id', None)
            secret_key = config.get('swfscheduler', 'aws_secret_access_key',
                                    None)
            if access_key is not None and secret_key is not None:
                kwargs['aws_access_key_id'] = access_key
                kwargs['aws_secret_access_key'] = secret_key
        if worker_idx is not None:
            if identity is not None:
                self.identity = '{0}-{1}'.format(identity, worker_idx)
            else:
                self.identity = str(worker_idx)
        else:
            if identity is not None:
                self.identity = str(identity)
            else:
                self.identity = None
        self.kwargs = kwargs

    def pid_file(self):
        """Path to the worker daemon's PID file, even if it is not running

        Append '-waiting' to this value to get the PID file path for a waiting
        process.

        :return: path to PID file
        :rtype: str
        """
        config = luigi.configuration.get_config()
        pid_dir = config.get('swfscheduler', 'worker-pid-file-dir')
        call(['mkdir', '-p', pid_dir])
        pidfile = 'swfworker-{0}.pid'.format(self.worker_idx)
        return os.path.join(pid_dir, pidfile)

    def _handle_term(self, s, f):
        logger.debug('WorkerServer()._handle_term()')
        self._got_term_signal = True

    def start(self):
        """Start the worker daemon and exit

        If there is already a worker daemon running with this index, this
        process will wait for that process to unlock its PID file before taking
        over. If there is already another process waiting to take over, the new
        one will send a ``SIGHUP`` to the old waiting process. This will not
        send any signals to the process that has locked the daemon PID file --
        it is your responsibility to call :meth:`stop` before calling this.
        This method will return immediately and not wait for the new daemon
        process to lock the PID file.

        See :meth:`pid_file` for the PID file and waiting PID file paths.

        :return: None
        """
        logstream = open(self.logfilename, 'a')
        logging.basicConfig(stream=logstream, level=self.loglevel,
                            format=self.logformat)
        config = luigi.configuration.get_config()
        waitconf = 'worker-pid-file-wait-sec'
        pid_wait = float(config.get('swfscheduler', waitconf, 10. * seconds))
        context = daemon.DaemonContext(
            pidfile=SingleWaitingLockPidFile(self.pid_file(), pid_wait),
            stdout=self.stdout,
            stderr=self.stderr,
            signal_map={
                signal.SIGWINCH: self._handle_term,
                signal.SIGTERM: 'terminate',
                signal.SIGHUP: 'terminate',
            },
            files_preserve=[logstream],
        )
        with context:
            worker = LuigiSwfWorker(**self.kwargs)
            while not self._got_term_signal:
                try:
                    worker.run(self.identity)
                except Exception:
                    tb = traceback.format_exc()
                    logger.error('WorkerServer().start(), error:\n%s', tb)
                sleep(0.001)

    def stop(self):
        """Shut down the worker daemon lazily from its PID file

        The worker daemon will exit in under 60 seconds if it is polling,
        or once the currently running activity task is finished. If it receives
        an activity task while waiting for the poll to finish, it will process
        the activity task and then exit. If there is a daemon process waiting
        to take over once the currently running one shuts down, this will send
        ``SIGHUP`` to the waiting process. This method will return immediately
        and will not wait for the processes to stop.

        :return: None
        """
        kill_from_pid_file(self.pid_file() + '-waiting', signal.SIGHUP)
        kill_from_pid_file(self.pid_file(), signal.SIGWINCH)
