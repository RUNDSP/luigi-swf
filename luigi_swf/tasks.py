import json
import logging

import luigi

from . import util


logger = logging.getLogger(__name__)


seconds = 1
minutes = 60 * seconds
hours = 60 * minutes


class SwfHeartbeatCancel(object):
    """Mix-in for Luigi Tasks

    Mix-in this class to a Luigi task to make use of SWF heartbeat timeouts and
    cancellation.

    When the task is being run directly in Luigi (not SWF),
    ``self.cancel_requested`` will always be ``False``, and calling
    :meth:`heartbeat` or :meth:`ack_cancel` will have no effect. However,
    your code should not call :meth:`ack_cancel` unless
    ``self.cancel_requested == True`` anyway.
    """

    cancel_requested = False
    cancel_acked = False

    def heartbeat(self):
        """Send heartbeat to SWF and check if cancellation was requested

        If cancellation was requested, ``self.cancel_requested`` will be set
        to ``True`` after invoking this method. This method has no effect
        when the task is not being run with SWF.
        """
        if not hasattr(self, 'activity_worker'):
            logger.debug('SwfHeartbeatCancel().heartbeat(), '
                         'worker not registered')
            return
        logger.debug('SwfHeartbeatCancel().heartbeat(), %s, heartbeat',
                     self.activity_id)
        if self.activity_worker.heartbeat()['cancelRequested']:
            logger.info('SwfHeartbeatCancel().heartbeat(), %s, '
                        'cancel requested', self.activity_id)
            self.cancel_requested = True

    def ack_cancel(self):
        """Send cancellation acknowledgement to SWF"""
        if not hasattr(self, 'activity_worker'):
            logger.debug('SwfHeartbeatCancel().ack_cancel(), '
                         'worker not registered')
            return
        logger.info("SwfHeartbeatCancel().ack_cancel(), %s, ack'ing cancel",
                    self.activity_id)
        self.activity_worker.cancel()
        self.cancel_acked = True

    def register_activity_worker(self, activity_worker, activity):
        """Register the activity worker as an observer of heartbeats

        Called by :class:`luigi_swf.worker.LuigiSwfWorker` to register itself
        as the activity worker managing this instance of the task. It observes
        :meth:`heartbeat` and :meth:`ack_cancel` from this class.
        """
        self.activity_worker = activity_worker
        self.activity_id = activity['activityId']


def get_task_configurations(task, include_obj=False):
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
    retries = getattr(task, 'swf_retries', 0)
    if not isinstance(retries, RetryBase):
        assert isinstance(retries, int)
        retries = RetryWait(max_failures=retries)
    tasks = {
        task.task_id: {
            'class': util.fullname(task),
            'task_family': task.task_family,
            'deps': [d.task_id for d in deps],
            'task_list': getattr(task, 'swf_task_list', 'default'),
            'params': json.dumps(util.get_luigi_params(task),
                                 default=util.dthandler),
            'retries': retries,
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
        tasks.update(get_task_configurations(dep))
    return tasks


class RetryBase(object):

    def get_retry_wait(self, failures):
        """
        Return `None` for no retry, otherwise time to wait in seconds.
        """
        raise NotImplementedError

    def __repr__(self):
        return self._repr()


class RetryWait(RetryBase):
    """Retry in `wait` seconds up to `max_failures - 1` times."""

    def __init__(self, wait=0, max_failures=None):
        self.wait = wait
        self.max_failures = max_failures

    def get_retry_wait(self, failures):
        if self.max_failures is None or failures < self.max_failures:
            return int(self.wait)

    def __eq__(self, other):
        return isinstance(other, self.__class__) \
            and other.wait == self.wait \
            and other.max_failures == self.max_failures

    def __ne__(self, other):
        return not self.__eq__(other)

    def _repr(self):
        return ('RetryWait(wait={}, max_failures={})'
                .format(self.wait, self.max_failures))


class RetryExponential(RetryBase):
    """Retry in `base ** failures` seconds up to `max_failures - 1` times."""

    def __init__(self, base=2.0, max_failures=None):
        self.base = base
        self.max_failures = max_failures

    def get_retry_wait(self, failures):
        if self.max_failures is None or failures < self.max_failures:
            return int(self.base ** failures)

    def __eq__(self, other):
        return isinstance(other, self.__class__) \
            and other.base == self.base \
            and other.max_failures == self.max_failures

    def __ne__(self, other):
        return not self.__eq__(other)

    def _repr(self):
        return ('RetryExponential(base={}, max_failures={})'
                .format(self.base, self.max_failures))
