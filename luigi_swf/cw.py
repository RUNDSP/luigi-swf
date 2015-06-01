import argparse

import boto.ec2.cloudwatch
from boto.ec2.cloudwatch.alarm import MetricAlarm


_get_cw_result = None


def get_cw():
    global _get_cw_result
    if _get_cw_result is None:
        _get_cw_result = boto.ec2.cloudwatch.connect_to_region('us-east-1')
    return _get_cw_result


cw_alarm_prefix = '(luigi-swf) '


class LuigiSWFAlarm(object):

    def alarm_name(self, task):
        raise NotImplementedError

    def alarm_params(self, task):
        raise NotImplementedError

    def create_alarm_obj(self, task):
        alarm = MetricAlarm(
            name=self.alarm_name(),
            **self.alarm_params())
        return alarm

    def update(self, task):
        alarm = self.create_alarm_obj(task)
        get_cw().put_metric_alarm(alarm)
        return alarm

    def activate(self, task):
        get_cw().enable_alarm_actions([self.alarm_name])

    def deactivate(self, task):
        get_cw().disable_alarm_actions([self.alarm_name])


class HasNotCompletedAlarm(LuigiSWFAlarm):

    def __init__(self, min_duration):
        self.min_duration = min_duration


class FailedAlarm(LuigiSWFAlarm):

    def __init__(self, min_failures=1, period=1):
        self.min_failures = min_failures
        self.period = period


class TimedOutAlarm(LuigiSWFAlarm):

    def __init__(self, min_timeouts=1, period=1):
        self.min_timeouts = min_timeouts
        self.period = period


class TaskHasNotCompletedAlarm(HasNotCompletedAlarm):

    def alarm_name(self, task):
        return '(luigi-swf) has not completed: {t} ({m})'.format(
            t=task.task_family,
            m=task.task_module)


class TaskFailedAlarm(FailedAlarm):

    def alarm_name(self, task):
        return '{pre}failed: {t} ({m})'.format(
            pre=cw_alarm_prefix,
            t=task.task_family,
            m=task.task_module)


class TaskTimedOutAlarm(TimedOutAlarm):

    def alarm_name(self, task):
        return '{pre}timed out: {t} ({m})'.format(
            pre=cw_alarm_prefix,
            t=task.task_family,
            m=task.task_module)


class WFHasNotCompletedAlarm(HasNotCompletedAlarm):

    def alarm_name(self, task):
        return '{pre}wf has not completed: {t} ({m})'.format(
            pre=cw_alarm_prefix,
            t=task.task_family,
            m=task.task_module)


class WFFailedAlarm(FailedAlarm):

    def alarm_name(self, task):
        return '{pre}wf failed: {t} ({m})'.format(
            pre=cw_alarm_prefix,
            t=task.task_family,
            m=task.task_module)


class WFTimedOutAlarm(TimedOutAlarm):

    def alarm_name(self, task):
        return '{pre}wf timed out: {t} ({m})'.format(
            pre=cw_alarm_prefix,
            t=task.task_family,
            m=task.task_module)


def cw_update_workflow(wf_task):
    pass


def cw_on(task):
    pass


def cw_off(task):
    pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser('Control CloudWatch monitoring')
    # TODO
