import argparse

import boto.ec2.cloudwatch
from boto.ec2.cloudwatch.alarm import MetricAlarm
import luigi.configuration


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
            name=self.alarm_name(),
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
        return alarm

    def activate(self, task):
        get_cw().enable_alarm_actions([self.alarm_name()])

    def deactivate(self, task):
        get_cw().disable_alarm_actions([self.alarm_name()])


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
            'metric_name': 'ActivityTasksCompleted',
            'evaluation_periods': self.min_duration,
            'comparison': '<=',
            'threshold': 0,
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
            'metric_name': 'ActivityTasksFailed',
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
            'metric_name': 'ActivityTasksTimedOut',
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
            'metric_name': 'WorkflowsCompleted',
            'evaluation_periods': self.min_duration,
            'comparison': '<=',
            'threshold': 0,
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
            'metric_name': 'WorkflowsFailed',
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
            'metric_name': 'WorkflowsTimedOut',
            'evaluation_periods': self.period,
            'comparison': '>=',
            'threshold': self.min_timeouts,
        }


def cw_update_workflow(wf_task):
    pass


def cw_on(task):
    pass


def cw_off(task):
    pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser('Control CloudWatch monitoring')
    # TODO
