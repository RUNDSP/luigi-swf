import collections
import logging
from time import sleep

import boto.ec2.cloudwatch
from boto.ec2.cloudwatch.alarm import MetricAlarm
import luigi
import luigi.configuration
from six import iteritems


logger = logging.getLogger(__name__)


_get_cw_result = None


def get_cw():
    global _get_cw_result
    if _get_cw_result is None:
        _get_cw_result = boto.ec2.cloudwatch.connect_to_region('us-east-1')
    return _get_cw_result


def batch(iterable, n=1):
    """
    http://stackoverflow.com/a/8290508/1118576
    """
    l = len(iterable)
    for ndx in range(0, l, n):
        yield iterable[ndx:min(ndx + n, l)]


def delete_alarms(alarms):
    for b in batch(alarms, 100):
        get_cw().delete_alarms(list(b))
        sleep(0.2)


cw_alarm_prefix = '(luigi-swf) '


class LuigiSWFAlarm(object):

    sns_topic_arns = NotImplemented
    evaluation_periods = NotImplemented
    period = NotImplemented

    def alarm_name(self, task):
        raise NotImplementedError

    def alarm_params(self, task, domain):
        raise NotImplementedError

    def create_alarm_obj(self, task, domain):
        alarm = MetricAlarm(
            name=self.alarm_name(task),
            alarm_actions=self.sns_topic_arns,
            namespace='AWS/SWF',
            period=self.period,
            evaluation_periods=self.evaluation_periods,
            statistic='Sum',
            **self.alarm_params(task, domain))
        return alarm

    def update(self, task):
        config = luigi.configuration.get_config()
        domain = config.get('swfscheduler', 'domain')
        alarm = self.create_alarm_obj(task, domain)
        get_cw().put_metric_alarm(alarm)
        return alarm

    def activate(self, task):
        get_cw().enable_alarm_actions([self.alarm_name(task)])

    def deactivate(self, task):
        get_cw().disable_alarm_actions([self.alarm_name(task)])


class HasNotCompletedAlarm(LuigiSWFAlarm):

    def __init__(self, sns_topic_arns, period, evaluation_periods=1):
        if period % 60 != 0:
            raise ValueError('period must be multiple of 60')
        self.sns_topic_arns = sns_topic_arns
        self.period = int(period)
        self.evaluation_periods = int(evaluation_periods)


class FailedAlarm(LuigiSWFAlarm):

    def __init__(self, sns_topic_arns, min_failures=1, period=60,
                 evaluation_periods=1):
        if period % 60 != 0:
            raise ValueError('period must be multiple of 60')
        self.sns_topic_arns = sns_topic_arns
        self.min_failures = min_failures
        self.period = int(period)
        self.evaluation_periods = int(evaluation_periods)


class TimedOutAlarm(LuigiSWFAlarm):

    def __init__(self, sns_topic_arns, min_timeouts=1, period=60,
                 evaluation_periods=1):
        if period % 60 != 0:
            raise ValueError('period must be multiple of 60')
        self.sns_topic_arns = sns_topic_arns
        self.min_timeouts = min_timeouts
        self.period = int(period)
        self.evaluation_periods = int(evaluation_periods)


class TaskHasNotCompletedAlarm(HasNotCompletedAlarm):

    def alarm_name(self, task):
        return '(luigi-swf) has not completed: {t} ({m})'.format(
            t=task.task_family,
            m=task.task_module)[:255]

    def alarm_params(self, task, domain):
        return {
            'dimensions': {
                'Domain': domain,
                'ActivityTypeName': task.task_family,
                'ActivityTypeVersion': 'unspecified',
            },
            'metric': 'ActivityTasksCompleted',
            'comparison': '<=',
            'threshold': 0,
            'insufficient_data_actions': self.sns_topic_arns,
        }


class TaskFailedAlarm(FailedAlarm):

    def alarm_name(self, task):
        return '{pre}failed: {t} ({m})'.format(
            pre=cw_alarm_prefix,
            t=task.task_family,
            m=task.task_module)[:255]

    def alarm_params(self, task, domain):
        return {
            'dimensions': {
                'Domain': domain,
                'ActivityTypeName': task.task_family,
                'ActivityTypeVersion': 'unspecified',
            },
            'metric': 'ActivityTasksFailed',
            'comparison': '>=',
            'threshold': self.min_failures,
        }


class TaskTimedOutAlarm(TimedOutAlarm):

    def alarm_name(self, task):
        return '{pre}timed out: {t} ({m})'.format(
            pre=cw_alarm_prefix,
            t=task.task_family,
            m=task.task_module)[:255]

    def alarm_params(self, task, domain):
        return {
            'dimensions': {
                'Domain': domain,
                'ActivityTypeName': task.task_family,
                'ActivityTypeVersion': 'unspecified',
            },
            'metric': 'ActivityTasksTimedOut',
            'comparison': '>=',
            'threshold': self.min_timeouts,
        }


class WFHasNotCompletedAlarm(HasNotCompletedAlarm):

    def alarm_name(self, task):
        return '{pre}wf has not completed: {t} ({m})'.format(
            pre=cw_alarm_prefix,
            t=task.task_family,
            m=task.task_module)[:255]

    def alarm_params(self, task, domain):
        return {
            'dimensions': {
                'Domain': domain,
                'WorkflowTypeName': task.task_family,
                'WorkflowTypeVersion': 'unspecified',
            },
            'metric': 'WorkflowsCompleted',
            'comparison': '<=',
            'threshold': 0,
            'insufficient_data_actions': self.sns_topic_arns,
        }


class WFFailedAlarm(FailedAlarm):

    def alarm_name(self, task):
        return '{pre}wf failed: {t} ({m})'.format(
            pre=cw_alarm_prefix,
            t=task.task_family,
            m=task.task_module)[:255]

    def alarm_params(self, task, domain):
        return {
            'dimensions': {
                'Domain': domain,
                'WorkflowTypeName': task.task_family,
                'WorkflowTypeVersion': 'unspecified',
            },
            'metric': 'WorkflowsFailed',
            'comparison': '>=',
            'threshold': self.min_failures,
        }


class WFTimedOutAlarm(TimedOutAlarm):

    def alarm_name(self, task):
        return '{pre}wf timed out: {t} ({m})'.format(
            pre=cw_alarm_prefix,
            t=task.task_family,
            m=task.task_module)[:255]

    def alarm_params(self, task, domain):
        return {
            'dimensions': {
                'Domain': domain,
                'WorkflowTypeName': task.task_family,
                'WorkflowTypeVersion': 'unspecified',
            },
            'metric': 'WorkflowsTimedOut',
            'comparison': '>=',
            'threshold': self.min_timeouts,
        }


def get_task_changes(task):
    puts = []
    f, to, nc, wf, wto, wnc = None, None, None, None, None, None
    for alarm in getattr(task, 'swf_cw_alarms', []):
        alarm.update(task)
        puts.append((alarm.alarm_name(task), (alarm, task)))
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
    return puts, deletes


def get_workflow_changes(task, updated=set()):
    deletes = []
    puts = []
    # Get alarm changes for all tasks.
    if not isinstance(task, luigi.WrapperTask) \
            and task.task_family not in updated:
        t_puts, t_deletes = get_task_changes(task)
        puts += t_puts
        deletes += t_deletes
    req = task.requires()
    if not isinstance(req, collections.Iterable):
        req = [req]
    for t in req:
        w_puts, w_deletes = get_workflow_changes(t, updated)
        puts += w_puts
        deletes += w_deletes
    return puts, deletes


def cw_update_workflow(task):
    puts, deletes = get_workflow_changes(task)
    puts = dict(puts)
    for alarm_name, put in iteritems(puts):
        # TODO: select existing ones first in batch
        #       and don't update unless necessary.
        alarm, task = put
        alarm.update(task)
        sleep(0.01)
    # TODO: also delete alarms which aren't in puts (source task is missing)
    #       based on prefix-based DescribeAlarms call from the above TODO.
    deletes = set(deletes)
    delete_alarms(deletes)
