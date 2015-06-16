from collections import Counter
import datetime
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

from .tasks import get_task_configurations
from .util import default_log_format, dthandler, kill_from_pid_file, \
    SingleWaitingLockPidFile, get_class, dt_from_iso


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


class WfState(object):

    def read_wf_state(self, events, task_configs):
        # Read what has happened.
        self.version = self._get_version(events)
        self.schedulings = self._get_all_schedulings(events)
        self.completed = self._get_completed(events, task_configs)
        self.failures = self._get_failures(events)
        self.last_fails = self._get_last_fails(events)
        self.timeouts = self._get_timeouts(events)
        self.retries = self._get_retries(events)
        self.wf_cancel_req = self._get_wf_cancel_requested(events)
        self.cancel_requests = self._get_task_cancel_requests(events)
        self.cancellations = self._get_activity_cancellations(events)
        # Make inferences.
        self.running = self._get_running()
        self.waiting = self._get_waiting()

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

    def _get_all_schedulings(self, events):
        all_schedulings = \
            [e[attrTaskScheduled]['activityId']
             for e in events
             if e['eventType'] == 'ActivityTaskScheduled']
        return Counter(all_schedulings)

    def _get_completed(self, events, task_configs):
        # Get completed activity tasks.
        compl_act = [self._get_task_id(events, e[attrTaskCompleted])
                     for e in events
                     if e['eventType'] == 'ActivityTaskCompleted']
        # Get completed wrappers.
        # Iterate so wrappers can depend on one another.
        completed_wrappers = []
        while True:
            completed = compl_act + completed_wrappers
            new_comp = [task_id
                        for task_id, task in iteritems(task_configs)
                        if (task['is_wrapper']
                            and task_id not in completed
                            and all(t in completed for t in task['deps']))]
            if len(new_comp) == 0:
                break
            completed_wrappers += new_comp
        return compl_act + completed_wrappers

    def _get_failures(self, events):
        return Counter([self._get_task_id(events, e[attrTaskFailed])
                        for e in events
                        if e['eventType'] == 'ActivityTaskFailed'])

    def _get_last_fails(self, events):
        last_fails = {}
        for e in events:
            if e['eventType'] == 'ActivityTaskFailed':
                task_id = self._get_task_id(events, e[attrTaskFailed])
                fail_time = datetime.datetime.utcfromtimestamp(
                    e['eventTimestamp'])
                if task_id in last_fails:
                    last_fails[task_id] = max(last_fails[task_id], fail_time)
                else:
                    last_fails[task_id] = fail_time
        return last_fails

    def _get_timeouts(self, events):
        return Counter([self._get_task_id(events, e[attrTaskTimedOut])
                        for e in events
                        if e['eventType'] == 'ActivityTaskTimedOut'])

    def _get_retries(self, events):
        def parse_retries(task_ids):
            try:
                return map(lambda s: s.strip(), task_ids.strip().split('\n'))
            except:
                tb = traceback.format_exc()
                logger.error('get_wf_state_full._get_retries'
                             '.parse_retries():\n%s', tb)
                return []

        retries_flat = \
            [retry
             for e in events
             if (e['eventType'] == 'WorkflowExecutionSignaled'
                 and e[attrWfSignaled]['signalName'] == 'retry')
             for retry in parse_retries(e[attrWfSignaled]['input'])]
        return Counter(retries_flat)

    def _get_wf_cancel_requested(self, events):
        return any(e for e in events
                   if e['eventType'] == 'WorkflowExecutionCancelRequested')

    def _get_task_cancel_requests(self, events):
        return [e[attrTaskCancReq]['activityId']
                for e in events
                if e['eventType'] == 'ActivityTaskCancelRequested']

    def _get_activity_cancellations(self, events):
        return Counter([self._get_task_id(events, e[attrTaskCanceled])
                        for e in events
                        if e['eventType'] == 'ActivityTaskCanceled'])

    def _get_running(self):
        return \
            [task_id
             for task_id, sched_count
             in iteritems(self.schedulings)
             if (task_id not in self.completed and
                 sched_count > (self.failures[task_id]
                                + self.timeouts[task_id]
                                + self.cancellations[task_id]))]

    def _get_waiting(self):
        return []  # TODO


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
            task_configs = self._get_task_configurations(events)
            state = WfState()
            state.read_wf_state(events, task_configs)
            self._decide(state, decisions, task_configs)
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

    def _decide(self, state, decisions, task_configs):
        if not state.wf_cancel_req:
            self._schedule_activities(state, decisions, task_configs)
        else:
            self._cancel_activities(state, decisions)

    def _get_events(self, decision_task):
        # It's paginated.
        events = decision_task['events']
        while 'nextPageToken' in decision_task:
            next_page_token = decision_task['nextPageToken']
            decision_task = self.poll(next_page_token=next_page_token)
            if 'events' in decision_task:
                events += decision_task['events']
        return events

    def _schedule_activities(self, state, decisions, task_configs):
        scheduled_count = 0
        runnables = self._get_runnables(state, task_configs)
        running_mutexes = self._get_running_mutexes(state, task_configs)
        now = datetime.datetime.utcnow()
        retryables, waitables, unretryables = \
            self._get_retryables(state, task_configs, now)
        self._schedule_retries(waitables, decisions)
        for task_id in runnables:
            task = task_configs[task_id]
            if task_id in unretryables:
                continue
            if task['running_mutex'] is not None:
                # These tasks want to run one-at-a-time per workflow execution.
                if task['running_mutex'] in running_mutexes:
                    continue
                running_mutexes.add(task['running_mutex'])
            scheduled_count += 1
            start_to_close = task['start_to_close_timeout']
            schedule_to_start = task['schedule_to_start_timeout']
            schedule_to_close = task['schedule_to_close_timeout']
            inp = {
                'class': task['class'],
                'params': task['params'],
            }
            decisions.schedule_activity_task(
                activity_id=task_id,
                activity_type_name=task['task_family'],
                activity_type_version=state.version,
                task_list=task['task_list'],
                heartbeat_timeout=str(task['heartbeat_timeout']),
                start_to_close_timeout=str(start_to_close),
                schedule_to_start_timeout=str(schedule_to_start),
                schedule_to_close_timeout=str(schedule_to_close),
                input=json.dumps(inp, default=dthandler))
            logger.debug('LuigiSwfDecider().run(), scheduled %s', task_id)
        if scheduled_count == 0 and len(state.running) == 0:
            if len(unretryables) > 0:
                msg = 'Task(s) failed: ' + ', '.join(unretryables)
                logger.error('LuigiSwfDecider().run(), '
                             'failing execution: %s', msg)
                decisions.fail_workflow_execution(reason=msg[:255],
                                                  details=msg[:32767])
            else:
                logger.debug('LuigiSwfDecider().run(), '
                             'completing workflow')
                decisions.complete_workflow_execution()

    def _schedule_retries(self, waitables, decisions):
        pass  # TODO

    def _cancel_activities(self, state, decisions):
        if len(state.running) > 0:
            for task_id in state.running:
                if task_id not in state.cancel_requests:
                    decisions.request_cancel_activity_task(task_id)
        else:
            decisions.cancel_workflow_executions()

    def _get_runnables(self, state, task_configs):
        result = []
        for task_id, task in iteritems(task_configs):
            if task_id not in state.completed and \
                    task_id not in state.running and \
                    all(d in state.completed for d in task['deps']):
                result.append(task_id)
        return result

    def _get_unretryables(self, state, task_configs):
        result = []
        for task_id, count in iteritems(state.failures):
            if count > task_configs[task_id]['retries'] + \
                    state.retries.get(task_id, 0):
                result.append(task_id)
        return result

    def _get_retryables(self, state, task_configs, now):
        retryables, waitables, unretryables = [], {}, []
        for task_id, count in iteritems(state.failures):
            retry_wait = task_configs[task_id]['retries'].get_retry_wait(
                count - state.retries.get(task_id, 0))
            if retry_wait is not None:
                retry_time = state.last_fails[task_id] + \
                    datetime.timedelta(seconds=retry_wait)
                if retry_wait == 0 or now >= retry_time:
                    retryables.append(task_id)
                elif task_id not in state.waiting:
                    waitables[task_id] = retry_wait
            else:
                unretryables.append(task_id)
        return retryables, waitables, unretryables

    def _get_task_configurations(self, events):
        wf_event = next(e for e in events
                        if e['eventType'] == 'WorkflowExecutionStarted')
        wf_event_attr = wf_event['workflowExecutionStartedEventAttributes']
        wf_input = json.loads(str(wf_event_attr['input']))
        wf_params = wf_input['wf_params']
        logger.debug('get_wf_state_full.get_task_configurations(), '
                     'wf_input:\n%s', pp.pformat(wf_input))
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
        return get_task_configurations(wf_task, include_obj=True)

    def _get_running_mutexes(self, state, task_configs):
        return \
            set(task['running_mutex']
                for task_id, task in iteritems(task_configs)
                if task_id in state.running and task['running_mutex'])


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
