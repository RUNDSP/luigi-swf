import argparse

import boto.ec2.cloudwatch


_get_cw_result = None


def get_cw():
    global _get_cw_result
    if _get_cw_result is None:
        _get_cw_result = boto.ec2.cloudwatch.connect_to_region('us-east-1')
    return _get_cw_result


cw_alarm_prefix = '(luigi-swf) '


class LuigiSWFAlarm(object):

    def query_cw_alarm(self, task):
        alarms = get_cw().describe_alarms(alarm_names=[self.alarm_name()])
        if len(alarms) > 0:
            return alarms[0]

    def alarm_name(self, task):
        raise NotImplementedError

    def activate(self):
        pass

    def deactivate(self):
        pass


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


def cw_update(tasks):
    pass


def cw_on(task):
    pass


def cw_off(task):
    pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser('Control CloudWatch monitoring')
    # TODO
