import logging
from time import sleep

import boto.ec2.cloudwatch
from boto.ec2.cloudwatch.alarm import MetricAlarm
import luigi
import luigi.configuration
from luigi.task import flatten
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
        deletes = list(b)
        logger.debug('delete_alarms(), deleting alarms %s', deletes)
        get_cw().delete_alarms(deletes)
        sleep(0.2)


cw_alarm_prefix = '(luigi-swf) '


def get_existing_alarms():
    r = get_cw().describe_alarms(alarm_name_prefix=cw_alarm_prefix)
    alarms = dict((a.name, a) for a in r)
    while r.next_token is not None:
        sleep(0.02)
        r = get_cw().describe_alarms(alarm_name_prefix=cw_alarm_prefix,
                                     next_token=r.next_token)
        for a in r:
            alarms[a.name] = a
    return alarms


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

    def update(self, task, prev_alarm=None):
        config = luigi.configuration.get_config()
        domain = config.get('swfscheduler', 'domain')
        alarm = self.create_alarm_obj(task, domain)
        if prev_alarm is not None and alarms_equal(alarm, prev_alarm):
            return False
        else:
            logger.debug("updating alarm '%s'", alarm.name)
            get_cw().put_metric_alarm(alarm)
            return True

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


def get_task_alarm_puts(task):
    puts = []
    for alarm in getattr(task, 'swf_cw_alarms', []):
        puts.append((alarm.alarm_name(task), (alarm, task)))
    return puts


def get_workflow_alarm_puts(task):
    puts = []
    # Get alarm changes for all tasks.
    if not isinstance(task, luigi.WrapperTask):
        puts += get_task_alarm_puts(task)
    req = flatten(task.requires())
    for t in req:
        puts += get_workflow_alarm_puts(t)
    return puts


def n2e(l):
    """None-to-empty (list)"""
    if l is None:
        return []
    return l


_alarm_equals_conditions = [
    lambda a: set(n2e(a[0].alarm_actions)) == set(n2e(a[1].alarm_actions)),
    lambda a: (set(n2e(a[0].insufficient_data_actions)) ==
               set(n2e(a[1].insufficient_data_actions))),
    lambda a: a[0].namespace == a[1].namespace,
    lambda a: a[0].period == a[1].period,
    lambda a: a[0].evaluation_periods == a[1].evaluation_periods,
    lambda a: a[0].statistic == a[1].statistic,
    lambda a: a[0].comparison == a[1].comparison,
    lambda a: a[0].threshold == a[1].threshold,
    lambda a: a[0].metric == a[1].metric,
    lambda a: a[0].dimensions == a[1].dimensions,
]


def alarms_equal(a1, a2):
    a = (a1, a2)
    return all(map(lambda f: f(a), _alarm_equals_conditions))


def cw_update_workflow(wf_tasks):
    if len(cw_alarm_prefix) == 0:
        raise RuntimeError('no cw_alarm_prefix. would delete all alarms.')
    logger.info('getting existing alarms')
    prev_alarms = get_existing_alarms()
    logger.info('getting alarm changes from workflow')
    puts = []
    for wf_task in wf_tasks:
        puts += get_workflow_alarm_puts(wf_task)
    puts = dict(puts)
    logger.info('updating alarms')
    for alarm_name, put in iteritems(puts):
        alarm, task = put
        prev_alarm = prev_alarms.get(alarm.alarm_name(task), None)
        if alarm.update(task, prev_alarm):
            sleep(0.01)
    logger.info('deleting not-needed alarms')
    deletes = [a for a in prev_alarms.keys()
               if a not in puts and a.startswith(cw_alarm_prefix)]
    delete_alarms(deletes)
