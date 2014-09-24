#!/usr/bin/env python

import datetime
import logging
import os.path
from subprocess import call

import luigi

from luigi_swf import LuigiSwfExecutor


logger = logging.getLogger(__name__)


seconds = 1.
minutes = 60. * seconds
hours = 60. * minutes


class DemoBasicTask(luigi.Task):

    # Workaround for when the task is in the same file you're executing
    __module__ = 'luigi_swf.examples.task_basic'

    dt = luigi.DateParameter()

    # Default values
    swf_task_list = 'default'
    swf_retries = 0
    swf_start_to_close_timeout = None  # in seconds
    swf_heartbeat_timeout = None  # in seconds

    def output(self):
        path = os.path.expanduser('~/luigi-swf-demo-basic-complete')
        return luigi.LocalTarget(path)

    def run(self):
        logger.info('hi | %s', self.dt)
        call(['touch', self.output().path])


class DemoBasicWorkflow(luigi.WrapperTask):

    dt = luigi.DateParameter()

    # Default values
    swf_wf_start_to_close_timeout = 15 * minutes  # in seconds

    def requires(self):
        return DemoBasicTask(dt=self.dt)


if __name__ == '__main__':
    task = DemoBasicWorkflow(dt=datetime.datetime(2000, 1, 1))
    domain = 'development'
    version = 'unspecified'
    ex = LuigiSwfExecutor(domain, version, task)
    ex.register()
    ex.execute()
