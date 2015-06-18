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
attrTaskStarted = 'activityTaskStartedEventAttributes'
attrTaskCompleted = 'activityTaskCompletedEventAttributes'
attrTaskFailed = 'activityTaskFailedEventAttributes'
attrTaskTimedOut = 'activityTaskTimedOutEventAttributes'
attrTaskCancReq = 'activityTaskCancelRequestedEventAttributes'
attrTaskCanceled = 'activityTaskCanceledEventAttributes'
attrWfSignaled = 'workflowExecutionSignaledEventAttributes'

seconds = 1.


def retry_timer_name(task_id):
    # We want to be able to see the task name for debugging, but sanitizing
    # it for the API could cause collisions, so we also include a hash.
    # 32-bit vs 64-bit: all deciders should run on the same for the hash.
    h = str(hash(task_id))
    tid = task_id \
        .replace('\\', '_') \
        .replace(':', '_') \
        .replace('|', '_') \
        .replace('arn', 'ARN')
    return 'retry-{}-{}'.format(h, tid)[:256]


class SWFEventHistoryCorruptedException(Exception):
    pass


class WfState(object):

    def __init__(self):
        self.version = None
        self.schedule_events = {}
        self.running = set()
        self.completed = set()
        self.schedulings = Counter()
        self.failures = Counter()
        self.last_fails = {}
        self.timeouts = Counter()
        self.retries = Counter()
        self.waiting = set()
        self.wf_cancel_req = False
        self.cancellations = Counter()
        self.cancel_requests = Counter()
        self.open_retry_timers = set()  # analogous to self.waiting

    def read_wf_state(self, events, task_configs):
        def parse_retries(task_ids):
            try:
                return map(lambda s: s.strip(), task_ids.strip().split('\n'))
            except:
                tb = traceback.format_exc()
                logger.error('get_wf_state_full._get_signaled_retries'
                             '.parse_retries():\n%s', tb)
                return []

        for e in events:
            if e['eventType'] == 'WorkflowExecutionStarted':
                self.version = (e['workflowExecutionStartedEventAttributes']
                                ['workflowType']['version'])
            elif e['eventType'] == 'ActivityTaskScheduled':
                tid = e['activityTaskScheduledEventAttributes']['activityId']
                self.schedule_events[e['eventId']] = tid
                self.schedulings[tid] += 1
            elif e['eventType'] == 'ActivityTaskScheduled':
                tid = e[attrTaskScheduled]['activityId']
                if tid in self.running:
                    raise SWFEventHistoryCorruptedException(
                        '(eventId={}) scheduled task was already running: {}'
                        .format(e['eventId'], repr(tid)))
                if tid in self.waiting:
                    self.waiting.remove(tid)
            elif e['eventType'] == 'ActivityTaskStarted':
                seid = e[attrTaskStarted]['scheduledEventId']
                tid = self.schedule_events[seid]
                if tid in self.running:
                    raise SWFEventHistoryCorruptedException(
                        '(eventId={}) started task was already running: {}'
                        .format(e['eventId'], repr(tid)))
                self.running.add(tid)
                if tid in self.waiting:
                    self.waiting.remove(tid)
            elif e['eventType'] == 'ActivityTaskCompleted':
                seid = e[attrTaskCompleted]['scheduledEventId']
                tid = self.schedule_events[seid]
                if tid not in self.running:
                    raise SWFEventHistoryCorruptedException(
                        '(eventId={}) completed task was not running: {}'
                        .format(e['eventId'], repr(tid)))
                self.running.remove(tid)
                self.completed.add(tid)
            elif e['eventType'] == 'ActivityTaskFailed':
                seid = e[attrTaskFailed]['scheduledEventId']
                tid = self.schedule_events[seid]
                if tid not in self.running:
                    raise SWFEventHistoryCorruptedException(
                        '(eventId={}) failed task was not running: {}'
                        .format(e['eventId'], repr(tid)))
                self.running.remove(tid)
                self.failures[tid] += 1
                fail_time = datetime.datetime.utcfromtimestamp(
                    e['eventTimestamp'])
                if tid in self.last_fails:
                    self.last_fails[tid] = max(self.last_fails[tid], fail_time)
                else:
                    self.last_fails[tid] = fail_time
            elif e['eventType'] == 'ActivityTaskTimedOut':
                seid = e[attrTaskTimedOut]['scheduledEventId']
                tid = self.schedule_events[seid]
                if tid not in self.running:
                    raise SWFEventHistoryCorruptedException(
                        '(eventId={}) timed out task was not running: {}'
                        .format(e['eventId'], repr(tid)))
                self.timeouts[tid] += 1
                self.running.remove(tid)
            elif e['eventType'] == 'TimerStarted':
                tid = e['timerStartedEventAttributes']['control']
                timer = e['timerStartedEventAttributes']['timerId']
                if tid in self.running:
                    raise SWFEventHistoryCorruptedException(
                        '(eventId={}) timer task was running: {}'
                        .format(e['eventId'], repr(tid)))
                if tid in self.waiting:
                    raise SWFEventHistoryCorruptedException(
                        '(eventId={}) timer task was already waiting: {}'
                        .format(e['eventId'], repr(tid)))
                if timer in self.open_retry_timers:
                    raise SWFEventHistoryCorruptedException(
                        '(eventId={}) started timer was running: {}'
                        .format(e['eventId'], repr(timer)))
                self.waiting.add(tid)
                self.open_retry_timers.add(timer)
            elif e['eventType'] == 'TimerFired':
                timer = e['timerFiredEventAttributes']['timerId']
                if timer not in self.open_retry_timers:
                    raise SWFEventHistoryCorruptedException(
                        '(eventId={}) fired timer had not started: {}'
                        .format(e['eventId'], repr(timer)))
                self.open_retry_timers.remove(timer)
            elif e['eventType'] == 'WorkflowExecutionSignaled':
                if e[attrWfSignaled]['signalName'] == 'retry':
                    for retry in parse_retries(e[attrWfSignaled]['input']):
                        self.retries[retry] += 1
            elif e['eventType'] == 'WorkflowExecutionCancelRequested':
                self.wf_cancel_req = True
            elif e['eventType'] == 'ActivityTaskCancelRequested':
                tid = e[attrTaskCancReq]['activityId']
                self.cancel_requests[tid] += 1
            elif e['eventType'] == 'ActivityTaskCanceled':
                tid = e[attrTaskCanceled]['activityId']
                if tid in self.running:
                    self.running.remove(tid)
                self.cancellations[tid] += 1
        # Get completed wrappers.
        # Iterate so wrappers can depend on one another.
        while True:
            new_comp = [task_id
                        for task_id, task in iteritems(task_configs)
                        if (task['is_wrapper']
                            and task_id not in self.completed
                            and all(t in self.completed for t in task['deps']))
                        ]
            if len(new_comp) == 0:
                break
            self.completed |= set(new_comp)


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
            now = datetime.datetime.utcnow()
            self._decide(state, decisions, task_configs, now)
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

    def _decide(self, state, decisions, task_configs, now):
        if not state.wf_cancel_req:
            self._schedule_activities(state, decisions, task_configs, now)
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

    def _schedule_activities(self, state, decisions, task_configs, now):
        scheduled = []
        deps_met = self._get_deps_met(state, task_configs)
        running_mutexes = self._get_running_mutexes(state, task_configs)
        retryables, waitables, unretryables = \
            self._get_retryables(state, task_configs, now)
        self._schedule_retries(waitables, decisions)
        for task_id in deps_met:
            if task_id in unretryables + list(waitables.keys()):
                continue
            task = task_configs[task_id]
            if task['running_mutex'] is not None:
                # These tasks want to run one-at-a-time per workflow execution.
                if task['running_mutex'] in running_mutexes:
                    continue
                running_mutexes.add(task['running_mutex'])
            scheduled.append(task_id)
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
        if len(scheduled + state.running + state.waiting
               + list(waitables.keys())) == 0:
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
        for task_id, wait in iteritems(waitables):
            wait_total = str(int(wait + 1 * seconds))
            logger.debug('LuigiSwfDecider, retrying %s in %s seconds',
                         task_id, wait_total)
            timer_name = retry_timer_name(task_id)
            decisions.start_timer(wait_total, timer_name, task_id)

    def _cancel_activities(self, state, decisions):
        if len(state.running) > 0:
            for task_id in state.running:
                if task_id not in state.cancel_requests:
                    decisions.request_cancel_activity_task(task_id)
        else:
            decisions.cancel_workflow_executions()

    def _get_deps_met(self, state, task_configs):
        result = []
        for task_id, task in iteritems(task_configs):
            if task_id not in state.completed and \
                    task_id not in state.running and \
                    all(d in state.completed for d in task['deps']):
                result.append(task_id)
        return result

    def _get_retryables(self, state, task_configs, now):
        retryables, waitables, unretryables = [], {}, []
        for task_id, count in iteritems(state.failures):
            retry_wait = task_configs[task_id]['retries'].get_retry_wait(
                count - state.signaled_retries.get(task_id, 0))
            if retry_wait is not None:
                retry_time = state.last_fails[task_id] + \
                    datetime.timedelta(seconds=retry_wait)
                if retry_wait == 0 or now >= retry_time:
                    logger.debug('_get_retryables, %s is retryable. '
                                 'retry_wait=%s, now=%s, retry_time=%s',
                                 task_id, retry_wait, now, retry_time)
                    retryables.append(task_id)
                elif task_id not in state.waiting:
                    waitables[task_id] = \
                        int((retry_time - now).total_seconds())
            else:
                unretryables.append(task_id)
        logger.debug('_get_retryables, retryables=%s', repr(retryables))
        logger.debug('_get_retryables, waitables=%s', repr(waitables))
        logger.debug('_get_retryables, unretryables=%s', repr(unretryables))
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
