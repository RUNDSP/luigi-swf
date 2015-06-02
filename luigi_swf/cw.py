import collections
import logging
from time import sleep

import boto.ec2.cloudwatch
from boto.ec2.cloudwatch.alarm import MetricAlarm
import luigi
import luigi.configuration


logger = logging.getLogger(__name__)


_get_cw_result = None


def get_cw():
    global _get_cw_result
    if _get_cw_result is None:
        _get_cw_result = boto.ec2.cloudwatch.connect_to_region('us-east-1')
    return _get_cw_result


cw_alarm_prefix = '(luigi-swf) '


class LuigiSWFAlarm(object):

    sns_topic_arns = NotImplemented

    def alarm_name(self, task):
        raise NotImplementedError

    def alarm_params(self, task, domain):
        raise NotImplementedError

    def create_alarm_obj(self, task, domain):
        alarm = MetricAlarm(
            name=self.alarm_name(task),
            alarm_actions=self.sns_topic_arns,
            namespace='AWS/SWF',
            period=60,
            statistic='Sum',
            **self.alarm_params(task, domain))
        return alarm

    def update(self, task):
        config = luigi.configuration.get_config()
        domain = config.get('swfscheduler', 'domain')
        alarm = self.create_alarm_obj(task, domain)
        get_cw().put_metric_alarm(alarm)
        sleep(0.1)
        return alarm

    def activate(self, task):
        get_cw().enable_alarm_actions([self.alarm_name(task)])

    def deactivate(self, task):
        get_cw().disable_alarm_actions([self.alarm_name(task)])


class HasNotCompletedAlarm(LuigiSWFAlarm):

    def __init__(self, sns_topic_arns, min_duration):
        self.sns_topic_arns = sns_topic_arns
        self.min_duration = min_duration


class FailedAlarm(LuigiSWFAlarm):

    def __init__(self, sns_topic_arns, min_failures=1, period=1):
        self.sns_topic_arns = sns_topic_arns
        self.min_failures = min_failures
        self.period = period


class TimedOutAlarm(LuigiSWFAlarm):

    def __init__(self, sns_topic_arns, min_timeouts=1, period=1):
        self.sns_topic_arns = sns_topic_arns
        self.min_timeouts = min_timeouts
        self.period = period


class TaskHasNotCompletedAlarm(HasNotCompletedAlarm):

    def alarm_name(self, task):
        return '(luigi-swf) has not completed: {t} ({m})'.format(
            t=task.task_family,
            m=task.task_module)

    def alarm_params(self, task, domain):
        return {
            'dimensions': {
                'Domain': domain,
                'ActivityTypeName': task.task_family,
                'ActivityTypeVersion': 'unspecified',
            },
            'metric': 'ActivityTasksCompleted',
            'evaluation_periods': self.min_duration,
            'comparison': '<=',
            'threshold': 0,
            'insufficient_data_actions': self.sns_topic_arns,
        }


class TaskFailedAlarm(FailedAlarm):

    def alarm_name(self, task):
        return '{pre}failed: {t} ({m})'.format(
            pre=cw_alarm_prefix,
            t=task.task_family,
            m=task.task_module)

    def alarm_params(self, task, domain):
        return {
            'dimensions': {
                'Domain': domain,
                'ActivityTypeName': task.task_family,
                'ActivityTypeVersion': 'unspecified',
            },
            'metric': 'ActivityTasksFailed',
            'evaluation_periods': self.period,
            'comparison': '>=',
            'threshold': self.min_failures,
        }


class TaskTimedOutAlarm(TimedOutAlarm):

    def alarm_name(self, task):
        return '{pre}timed out: {t} ({m})'.format(
            pre=cw_alarm_prefix,
            t=task.task_family,
            m=task.task_module)

    def alarm_params(self, task, domain):
        return {
            'dimensions': {
                'Domain': domain,
                'ActivityTypeName': task.task_family,
                'ActivityTypeVersion': 'unspecified',
            },
            'metric': 'ActivityTasksTimedOut',
            'evaluation_periods': self.period,
            'comparison': '>=',
            'threshold': self.min_timeouts,
        }


class WFHasNotCompletedAlarm(HasNotCompletedAlarm):

    def alarm_name(self, task):
        return '{pre}wf has not completed: {t} ({m})'.format(
            pre=cw_alarm_prefix,
            t=task.task_family,
            m=task.task_module)

    def alarm_params(self, task, domain):
        return {
            'dimensions': {
                'Domain': domain,
                'WorkflowTypeName': task.task_family,
                'WorkflowTypeVersion': 'unspecified',
            },
            'metric': 'WorkflowsCompleted',
            'evaluation_periods': self.min_duration,
            'comparison': '<=',
            'threshold': 0,
            'insufficient_data_actions': self.sns_topic_arns,
        }


class WFFailedAlarm(FailedAlarm):

    def alarm_name(self, task):
        return '{pre}wf failed: {t} ({m})'.format(
            pre=cw_alarm_prefix,
            t=task.task_family,
            m=task.task_module)

    def alarm_params(self, task, domain):
        return {
            'dimensions': {
                'Domain': domain,
                'WorkflowTypeName': task.task_family,
                'WorkflowTypeVersion': 'unspecified',
            },
            'metric': 'WorkflowsFailed',
            'evaluation_periods': self.period,
            'comparison': '>=',
            'threshold': self.min_failures,
        }


class WFTimedOutAlarm(TimedOutAlarm):

    def alarm_name(self, task):
        return '{pre}wf timed out: {t} ({m})'.format(
            pre=cw_alarm_prefix,
            t=task.task_family,
            m=task.task_module)

    def alarm_params(self, task, domain):
        return {
            'dimensions': {
                'Domain': domain,
                'WorkflowTypeName': task.task_family,
                'WorkflowTypeVersion': 'unspecified',
            },
            'metric': 'WorkflowsTimedOut',
            'evaluation_periods': self.period,
            'comparison': '>=',
            'threshold': self.min_timeouts,
        }


def batch(iterable, n=1):
    """
    http://stackoverflow.com/a/8290508/1118576
    """
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


def cw_update_task(task, return_deletes=False):
    f, to, nc, wf, wto, wnc = None, None, None, None, None, None
    for alarm in getattr(task, 'swf_cw_alarms', []):
        alarm.update(task)
        if isinstance(alarm, TaskFailedAlarm):
            f = alarm
        elif isinstance(alarm, TaskTimedOutAlarm):
            to = alarm
        elif isinstance(alarm, TaskHasNotCompletedAlarm):
            nc = alarm
        elif isinstance(alarm, WFFailedAlarm):
            wf = alarm
        elif isinstance(alarm, WFTimedOutAlarm):
            wto = alarm
        elif isinstance(alarm, WFHasNotCompletedAlarm):
            wnc = alarm
        else:
            logger.warn("Won't be able to delete this alarm when it's "
                        "unused: %s", alarm.name)
    deletes = []
    if f is None:
        deletes.append(TaskFailedAlarm([]).alarm_name(task))
    if to is None:
        deletes.append(TaskTimedOutAlarm([]).alarm_name(task))
    if nc is None:
        deletes.append(TaskHasNotCompletedAlarm([], 1).alarm_name(task))
    if wf is None:
        deletes.append(WFFailedAlarm([]).alarm_name(task))
    if wto is None:
        deletes.append(WFTimedOutAlarm([]).alarm_name(task))
    if wnc is None:
        deletes.append(WFHasNotCompletedAlarm([], 1).alarm_name(task))
    if return_deletes:
        return deletes
    else:
        for b in batch(deletes, 10):
            get_cw().delete_alarms(list(b))
            sleep(0.1)


def cw_update_workflow(task, updated=set()):
    deletes = []
    # Update present alarms.
    if not isinstance(task, luigi.WrapperTask) \
            and task.task_family not in updated:
        deletes += cw_update_task(task, return_deletes=True)
        updated.add(task.task_family)
    req = task.requires()
    if isinstance(req, collections.Iterable):
        for t in req:
            updated = cw_update_workflow(t, updated)
    elif isinstance(req, luigi.Task):
        updated = cw_update_workflow(req, updated)
    # Delete missing alarms.
    for b in batch(deletes, 10):
        get_cw().delete_alarms(list(b))
        sleep(0.1)
    return updated
