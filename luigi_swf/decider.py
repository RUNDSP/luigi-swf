from collections import Counter
import json
import logging
import os
import os.path
import pprint
import signal
from subprocess import call
from time import sleep
import traceback

import boto.swf.layer2 as swf
import daemon
import luigi.configuration
from six import iteritems

from .util import default_log_format, dthandler, kill_from_pid_file, \
    SingleWaitingLockPidFile, get_all_tasks, get_class, dt_from_iso


logger = logging.getLogger(__name__)
pp = pprint.PrettyPrinter(indent=2)


attrTaskScheduled = 'activityTaskScheduledEventAttributes'
attrTaskCompleted = 'activityTaskCompletedEventAttributes'
attrTaskFailed = 'activityTaskFailedEventAttributes'
attrTaskTimedOut = 'activityTaskTimedOutEventAttributes'
attrTaskCancReq = 'activityTaskCancelRequestedEventAttributes'
attrTaskCanceled = 'activityTaskCanceledEventAttributes'
attrWfSignaled = 'workflowExecutionSignaledEventAttributes'

seconds = 1.


class LuigiSwfDecider(swf.Decider):
    """Implementation of boto's SWF Decider

    See :class:`DeciderServer` for daemonizing this.
    """

    def run(self, identity=None):
        """Poll for and run a decision task

        This should be run in a loop. It will poll for up to 60 seconds. After
        60 seconds, it will return without running any decision tasks. The user
        should usually not need to interact with this class directly. Instead,
        :class:`DeciderServer` can be used to run the loop.

        :return: None
        """
        decision_task = self.poll(identity=identity)
        decisions = swf.Layer1Decisions()
        try:
            if 'events' not in decision_task:
                logger.debug('LuigiSwfDecider().run(), poll timed out')
                return
            events = self._get_events(decision_task)
            state = self._get_state(events)
            if not state['wf_cancel_req']:
                version = self._get_version(events)
                self._schedule_activities(state, decisions, version)
            else:
                self._cancel_activities(state, decisions)
            self.complete(decisions=decisions)
        except Exception as error:
            tb = traceback.format_exc()
            reason = ('Decider failed:\n' + str(error))[:255]
            details = (tb + '\n' + str(error))[:32767]
            logger.error('LuigiSwfDecider().run(), decider failed:\n%s',
                         details)
            decisions.fail_workflow_execution(reason=reason, details=details)
            self.complete(decisions=decisions)
            raise

    def _get_events(self, decision_task):
        # It's paginated.
        events = decision_task['events']
        while 'nextPageToken' in decision_task:
            next_page_token = decision_task['nextPageToken']
            decision_task = self.poll(next_page_token=next_page_token)
            if 'events' in decision_task:
                events += decision_task['events']
        return events

    def _get_all_tasks(self, events):
        wf_event = next(e for e in events
                        if e['eventType'] == 'WorkflowExecutionStarted')
        wf_event_attr = wf_event['workflowExecutionStartedEventAttributes']
        wf_input = json.loads(str(wf_event_attr['input']))
        wf_params = wf_input['wf_params']
        logger.debug('LuigiSwfDecider()._get_all_tasks(), wf_input:\n%s',
                     pp.pformat(wf_input))
        wf_cls = get_class(*wf_input['wf_task'])
        kwargs = dict()
        for param_name, param_cls in wf_cls.get_params():
            if param_name == 'pool':
                continue
            if isinstance(param_cls, luigi.DateParameter):
                kwargs[param_name] = dt_from_iso(wf_params[param_name])
            else:
                kwargs[param_name] = wf_params[param_name]
        wf_task = wf_cls(**kwargs)
        all_tasks = get_all_tasks(wf_task, include_obj=True)
        return all_tasks

    def _get_version(self, events):
        wf_event = next(e for e in events
                        if e['eventType'] == 'WorkflowExecutionStarted')
        wf_event_attr = wf_event['workflowExecutionStartedEventAttributes']
        return wf_event_attr['workflowType']['version']

    def _get_task_id(self, events, event_attributes):
        event_id = event_attributes['scheduledEventId'] - 1
        activity = events[event_id]
        attributes = activity['activityTaskScheduledEventAttributes']
        return attributes['activityId']

    def _get_runnables(self, all_tasks, completed, running):
        """
        >>> all_tasks = {
        ...     'Task1': {'deps': ['Task3']},
        ...     'Task2': {'deps': ['Task3']},
        ...     'Task3': {'deps': []},
        ...     'Task4': {'deps': []},
        ... }
        >>> completed = ['Task3']
        >>> running = ['Task4']
        >>> decider = LuigiSwfDecider()
        >>> list(decider._get_runnables(all_tasks, completed, running))
        [('Task1', {'deps': ['Task3']}), ('Task2', {'deps': ['Task3']})]
        >>> completed = ['Task1']
        >>> list(decider._get_runnables(all_tasks, completed))
        [('Task3', {'deps': []})]
        """
        for task_id, task in iteritems(all_tasks):
            if task_id not in completed and task_id not in running and \
                    all(d in completed for d in task['deps']):
                yield task_id, task

    def _get_unretryables(self, all_tasks, failed, retries):
        for task_id, count in iteritems(failed):
            if count > all_tasks[task_id]['retries'] + retries.get(task_id, 0):
                yield task_id

    def _get_completed_wrappers(self, all_tasks, completed_activities):
        # Iterate so wrappers can depend on one another.
        completed_wrappers = []
        while True:
            completed = completed_activities + completed_wrappers
            new_comp = [task_id
                        for task_id, task in iteritems(all_tasks)
                        if (task['is_wrapper']
                            and task_id not in completed
                            and all(t in completed for t in task['deps']))]
            if len(new_comp) == 0:
                break
            completed_wrappers += new_comp
        return completed_wrappers

    def _get_state(self, events):
        all_tasks = self._get_all_tasks(events)
        state = dict()
        state['all_schedulings'] = \
            [e[attrTaskScheduled]['activityId']
             for e in events
             if e['eventType'] == 'ActivityTaskScheduled']
        state['schedulings'] = Counter(state['all_schedulings'])
        state['compl_act'] = [self._get_task_id(events, e[attrTaskCompleted])
                              for e in events
                              if e['eventType'] == 'ActivityTaskCompleted']
        state['compl_w'] = self._get_completed_wrappers(all_tasks,
                                                        state['compl_act'])
        state['completed'] = state['compl_act'] + state['compl_w']
        failures_flat = [self._get_task_id(events, e[attrTaskFailed])
                         for e in events
                         if e['eventType'] == 'ActivityTaskFailed']
        state['failures'] = Counter(failures_flat)
        timeouts_flat = [self._get_task_id(events, e[attrTaskTimedOut])
                         for e in events
                         if e['eventType'] == 'ActivityTaskTimedOut']
        state['timeouts'] = Counter(timeouts_flat)
        retries_flat = \
            [retry
             for e in events
             if (e['eventType'] == 'WorkflowExecutionSignaled'
                 and e[attrWfSignaled]['signalName'] == 'retry')
             for retry in self._parse_retries(e[attrWfSignaled]['input'])]
        state['retries'] = Counter(retries_flat)
        state['wf_cancel_req'] = any(e for e in events
                                     if e['eventType'] == ('WorkflowExecution'
                                                           'CancelRequested'))
        state['cancel_requests'] = [e[attrTaskCancReq]['activityId']
                                    for e in events
                                    if e['eventType'] == ('ActivityTask'
                                                          'CancelRequested')]
        act_cancels = [self._get_task_id(events,
                                         e[attrTaskCanceled])
                       for e in events
                       if e['eventType'] == ('ActivityTask'
                                             'Canceled')]
        state['cancellations'] = Counter(act_cancels)
        state['running'] = \
            [task_id
             for task_id, sched_count
             in iteritems(state['schedulings'])
             if (task_id not in state['completed'] and
                 sched_count > (state['failures'][task_id]
                                + state['timeouts'][task_id]
                                + state['cancellations'][task_id]))]
        state['running_mutexes'] = \
            set(task['running_mutex']
                for task_id, task in iteritems(all_tasks)
                if task_id in state['running'] and task['running_mutex'])
        state['runnables'] = list(self._get_runnables(all_tasks,
                                                      state['completed'],
                                                      state['running']))
        state['unretryables'] = list(self._get_unretryables(all_tasks,
                                                            state['failures'],
                                                            state['retries']))
        logger.debug('LuigiSwfDecider().get_state(), state:\n%s',
                     pp.pformat(state))
        return state

    def _parse_retries(self, task_ids):
        try:
            return map(lambda s: s.strip(), task_ids.strip().split('\n'))
        except:
            tb = traceback.format_exc()
            logger.error('LuigiSwfDecider()._parse_retries():\n%s', tb)
            return []

    def _schedule_activities(self, state, decisions, version):
        scheduled_count = 0
        for task_id, task in state['runnables']:
            if task_id in state['unretryables']:
                continue
            if task['running_mutex'] is not None:
                # These tasks want to run one-at-a-time per workflow execution.
                if task['running_mutex'] in state['running_mutexes']:
                    continue
                state['running_mutexes'].add(task['running_mutex'])
            scheduled_count += 1
            start_to_close = task['start_to_close_timeout']
            schedule_to_start = task['schedule_to_start_timeout']
            schedule_to_close = task['schedule_to_close_timeout']
            decisions.schedule_activity_task(
                activity_id=task_id,
                activity_type_name=task['task_family'],
                activity_type_version=version,
                task_list=task['task_list'],
                heartbeat_timeout=str(task['heartbeat_timeout']),
                start_to_close_timeout=str(start_to_close),
                schedule_to_start_timeout=str(schedule_to_start),
                schedule_to_close_timeout=str(schedule_to_close),
                input=json.dumps(task, default=dthandler))
            logger.debug('LuigiSwfDecider().run(), scheduled %s', task_id)
        if scheduled_count == 0 and len(state['running']) == 0:
            if len(state['unretryables']) > 0:
                msg = 'Task(s) failed: ' + ', '.join(state['unretryables'])
                logger.error('LuigiSwfDecider().run(), '
                             'failing execution: %s', msg)
                decisions.fail_workflow_execution(reason=msg[:255],
                                                  details=msg[:32767])
            else:
                logger.debug('LuigiSwfDecider().run(), '
                             'completing workflow')
                decisions.complete_workflow_execution()

    def _cancel_activities(self, state, decisions):
        if len(state['running']) > 0:
            for task_id in state['running']:
                if task_id not in state['cancel_requests']:
                    decisions.request_cancel_activity_task(task_id)
        else:
            decisions.cancel_workflow_executions()


class DeciderServer(object):
    """Decider daemon

    Daemonizes :class:`LuigiSwfDecider`. The ``SIGWINCH`` signal is used to
    shut this down lazily (after processing the current decision task or
    60-second poll) because ``SIGTERM`` kills child processes.

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

    def __init__(self, identity=None, stdout=None, stderr=None,
                 logfilename=None, loglevel=logging.INFO,
                 logformat=default_log_format, **kwargs):
        logger.debug('DeciderServer.__init__(...)')
        config = luigi.configuration.get_config()
        if stdout is None:
            stdout_path = config.get('swfscheduler', 'decider-log-out')
            self.stdout = open(stdout_path, 'a')
        else:
            self.stdout = stdout
        if stderr is None:
            stderr_path = config.get('swfscheduler', 'decider-log-err')
            self.stderr = open(stderr_path, 'a')
        else:
            self.stderr = stderr
        if logfilename is not None:
            self.logfilename = logfilename
        else:
            self.logfilename = config.get('swfscheduler', 'decider-log')
        self.loglevel = loglevel
        self.logformat = logformat
        if 'domain' not in kwargs:
            kwargs['domain'] = config.get('swfscheduler', 'domain')
        if 'task_list' not in kwargs:
            kwargs['task_list'] = 'luigi'
        if 'aws_access_key_id' not in kwargs or \
                'aws_secret_access_key' not in kwargs:
            access_key = config.get('swfscheduler', 'aws_access_key_id', None)
            secret_key = config.get('swfscheduler', 'aws_secret_access_key',
                                    None)
            if access_key is not None and secret_key is not None:
                kwargs['aws_access_key_id'] = access_key
                kwargs['aws_secret_access_key'] = secret_key
        self.identity = identity
        self.kwargs = kwargs

    def pid_file(self):
        """Path to the decider daemon's PID file, even if it is not running

        Append '-waiting' to this value to get the PID file path for a waiting
        process.

        :return: path to PID file
        :rtype: str
        """
        config = luigi.configuration.get_config()
        pid_dir = config.get('swfscheduler', 'decider-pid-file-dir')
        call(['mkdir', '-p', pid_dir])
        return os.path.join(pid_dir, 'swfdecider.pid')

    def _handle_term(self, s, f):
        logger.debug('DeciderServer()._handle_term()')
        self._got_term_signal = True

    def start(self):
        """Start the decider daemon and exit

        If there is already a decider daemon running, this process will wait
        for that process to unlock its PID file before taking over. If there is
        already another process waiting to take over, the new one will send a
        ``SIGHUP`` to the old waiting process. This will not send any signals
        to the process that has locked the daemon PID file -- it is your
        responsibility to call :meth:`stop` before calling this. This method
        will return immediately and not wait for the new daemon process to lock
        the PID file.

        See :meth:`pid_file` for the PID file and waiting PID file paths.

        :return: None
        """
        logstream = open(self.logfilename, 'a')
        logging.basicConfig(stream=logstream, level=self.loglevel,
                            format=self.logformat)
        config = luigi.configuration.get_config()
        waitconf = 'decider-pid-file-wait-sec'
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
            decider = LuigiSwfDecider(**self.kwargs)
            while not self._got_term_signal:
                try:
                    logger.debug('DeciderServer().start(), decider.run()')
                    decider.run(self.identity)
                except Exception:
                    tb = traceback.format_exc()
                    logger.error('DeciderServer().start(), error:\n%s', tb)
                sleep(0.001 * seconds)

    def stop(self):
        """Shut down the decider daemon lazily from its PID file

        The decider daemon will exit in under 60 seconds if it is polling,
        or once the currently running decision task is finished. If it receives
        a decision task while waiting for the poll to finish, it will process
        the decision task and then exit. If there is a daemon process waiting
        to take over once the currently running one shuts down, this will send
        ``SIGHUP`` to the waiting process. This method will return immediately
        and will not wait for the processes to stop.

        :return: None
        """
        kill_from_pid_file(self.pid_file() + '-waiting', signal.SIGHUP)
        kill_from_pid_file(self.pid_file(), signal.SIGWINCH)
