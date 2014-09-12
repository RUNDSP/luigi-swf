#!/usr/bin/env python

import datetime
import logging
import os.path
from subprocess import call
from time import sleep

import luigi
from luigi_swf import LuigiSwfExecutor, SwfHeartbeatCancel


logger = logging.getLogger(__name__)


seconds = 1.
minutes = 60. * seconds
hours = 60. * minutes


class DemoCancelTask(luigi.Task, SwfHeartbeatCancel):
    """
    While this is running, try this:
    1. `tail -f /path/to/logs/worker-N` (worker identity number in SWF console)
    2. Cancel the workflow in the SWF console with the "Try-cancel" button
    """

    # Workaround for when the task is in the same file you're executing
    __module__ = 'luigi_swf.examples.task_cancel'

    swf_task_list = 'workflow'

    def output(self):
        path = os.path.expanduser('~/luigi-swf-demo-cancel-complete')
        return luigi.LocalTarget(path)

    def run(self):
        times = 30
        while True:
            logger.info('times: %s', times)
            self.heartbeat()
            if self.cancel_requested:
                logger.info('cancel requested')
                self.ack_cancel()
                return
            times -= 1
            if times == 0:
                break
            sleep(15 * seconds)
        logger.info('done')
        call(['touch', self.output().path])


class DemoCancelWorkflow(luigi.WrapperTask):

    swf_wf_start_to_close_timeout = 1 * hours

    def requires(self):
        return DemoCancelTask()


if __name__ == '__main__':
    task = DemoCancelWorkflow()
    domain = 'dmp_staging'
    version = 'unspecified'
    ex = LuigiSwfExecutor(domain, version, task)
    ex.register()
    ex.execute()
