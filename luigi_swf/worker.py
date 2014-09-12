import datetime
import json
import logging
import os
import os.path
import signal
from subprocess import call
import sys
from time import sleep
import traceback
import types

import arrow
import boto.swf.layer2 as swf
import daemon
import luigi
import luigi.configuration

from .tasks import SwfHeartbeatCancel
from .util import default_log_format, get_class, kill_from_pid_file, \
    SingleWaitingLockPidFile


logger = logging.getLogger(__name__)


seconds = 1.


class LuigiSwfWorker(swf.ActivityWorker):

    def run(self, identity):
        activity_task = self.poll(identity=identity)
        if 'activityId' not in activity_task:
            logger.debug('LuigiSwfWorker().run(), poll timed out')
            return
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
                    kwargs[param_name] = arrow.get(input_params[param_name])
                else:
                    kwargs[param_name] = input_params[param_name]
            task = task_cls(**kwargs)
            if task.complete():
                result = 'Did not run (task.complete() returned true)'
                logger.debug('LuigiSwfWorker().run(), %s, %s', result)
                self.complete(result=result)
                return
            if hasattr(task, 'register_activity_worker'):
                task.register_activity_worker(self,
                                              activity_task['activityId'])
            task.run()
            task_completed = task.complete()
        except Exception, error:
            tb = traceback.format_exc()
            logger.error('LuigiSwfWorker().run(), %s, error:\n%s',
                         activity_task['activityId'], tb)
            details = tb + '\n\n\n' + str(error)
            self.fail(reason=str(error)[:255], details=details[:32767])
            raise
        if task_completed:
            self.complete()
            logger.info('LuigiSwfWorker().run(), completed %s',
                        activity_task['activityId'])
        else:
            reason = 'complete() returned false after running'
            logger.error('LuigiSwfWorker().run(), %s, failed (%s)',
                         activity_task['activityId'], reason)
            self.fail(reason=reason)


class WorkerServer(object):
    """
    Shut down with SIGWINCH because SIGTERM kills subprocesses even if we
    handle it with the signal map.
    """

    got_term_signal = False

    def __init__(self, worker_idx, stdout=None, stderr=None, logfilename=None,
                 loglevel=logging.INFO, logformat=default_log_format,
                 **kwargs):
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
        self.identity = 'worker-' + str(worker_idx)
        self.kwargs = kwargs

    def _pid_file(self):
        config = luigi.configuration.get_config()
        pid_dir = config.get('swfscheduler', 'worker-pid-file-dir')
        call(['mkdir', '-p', pid_dir])
        pidfile = 'swfworker-{0}.pid'.format(self.worker_idx)
        return os.path.join(pid_dir, pidfile)

    def _handle_term(self, s, f):
        logger.debug('WorkerServer()._handle_term()')
        self.got_term_signal = True

    def start(self):
        logstream = open(self.logfilename, 'a')
        logging.basicConfig(stream=logstream, level=self.loglevel,
                            format=self.logformat)
        config = luigi.configuration.get_config()
        waitconf = 'worker-pid-file-wait-sec'
        pid_wait = float(config.get('swfscheduler', waitconf, 10. * seconds))
        context = daemon.DaemonContext(
            pidfile=SingleWaitingLockPidFile(self._pid_file(), pid_wait),
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
            while not self.got_term_signal:
                try:
                    worker.run(self.identity)
                except Exception as ex:
                    tb = traceback.format_exc()
                    logger.error('WorkerServer().start(), error:\n%s', tb)
                sleep(0.001)

    def stop(self):
        kill_from_pid_file(self._pid_file() + '-waiting', signal.SIGHUP)
        kill_from_pid_file(self._pid_file(), signal.SIGWINCH)
