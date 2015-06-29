#!/usr/bin/env python

import datetime
import logging
import os.path
from subprocess import call

import luigi

from luigi_swf import cw, LuigiSwfExecutor


logger = logging.getLogger(__name__)


seconds = 1.
minutes = 60. * seconds
hours = 60. * minutes


class DemoBasicTask(luigi.Task):

    # Workaround for when the task is in the same file you're executing
    __module__ = 'luigi_swf.examples.task_basic'

    dt = luigi.DateParameter()
    hour = luigi.IntParameter()

    # Default values
    swf_task_list = 'default'
    swf_retries = 0
    swf_start_to_close_timeout = None  # in seconds
    swf_heartbeat_timeout = None  # in seconds

    # Use luigi_swf.cw.cw_update_workflows() to sync these to CloudWatch.
    swf_cw_alarms = [
        cw.TaskFailedAlarm(['arn:aws:sns:us-east-1:1234567:alert_ops']),
        cw.TaskFailedAlarm(['arn:aws:sns:us-east-1:1234567:alert_ops']),
        cw.TaskHasNotCompletedAlarm(
            ['arn:aws:sns:us-east-1:1234567:alert_ops'], period=2.5 * hours),
    ]

    def output(self):
        path = os.path.expanduser('~/luigi-swf-demo-basic-complete')
        return luigi.LocalTarget(path)

    def run(self):
        logger.info('hi | %s', self.dt)
        call(['touch', self.output().path])


class DemoBasicWorkflow(luigi.WrapperTask):

    dt = luigi.DateParameter()
    hour = luigi.IntParameter()

    def requires(self):
        return DemoBasicTask(dt=self.dt, hour=self.hour)


if __name__ == '__main__':
    task = DemoBasicWorkflow(dt=datetime.datetime(2000, 1, 1), hour=0)
    domain = 'development'
    version = 'unspecified'
    ex = LuigiSwfExecutor(domain, version, task)
    ex.register()
    ex.execute()
