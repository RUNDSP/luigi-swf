import json
import logging
import cPickle as pickle
import pprint

import boto.swf.layer2 as swf
from boto.swf.exceptions import SWFTypeAlreadyExistsError, \
    SWFDomainAlreadyExistsError
import luigi
import luigi.configuration
from six import iteritems, print_

from .util import dthandler, fullname, get_luigi_params


logger = logging.getLogger(__name__)
pp = pprint.PrettyPrinter(indent=2)


class LuigiSwfExecutor(object):

    def __init__(self, domain, version, workflow_task,
                 aws_access_key_id=None, aws_secret_access_key=None):
        assert isinstance(version, basestring), "version must be a string"
        logger.debug('LuigiSwfExecutor.__init__(domain=%s, version=%s, '
                     'workflow_task=%s, '
                     'aws_access_key_id=..., aws_secret_access_key=...)',
                     repr(domain), repr(version), repr(workflow_task))
        self.domain = domain
        self.version = version
        self.workflow_task = workflow_task
        config = luigi.configuration.get_config()
        if aws_access_key_id is None:
            aws_access_key_id = config.get('swfscheduler', 'aws_access_key_id',
                                           None)
            if aws_access_key_id is not None:
                logger.debug('Read aws_access_key_id from config')
            else:
                logger.debug('Boto will determine aws_access_key_id')
        if aws_secret_access_key is None:
            aws_secret_access_key = config.get('swfscheduler',
                                               'aws_secret_access_key', None)
            if aws_secret_access_key is not None:
                logger.debug('Read aws_secret_access_key from config')
            else:
                logger.debug('Boto will determine aws_secret_access_key')
        if aws_access_key_id is not None and aws_secret_access_key is not None:
            swf.set_default_credentials(aws_access_key_id,
                                        aws_secret_access_key)

    def _get_all_tasks(self, task):
        deps = task.deps()
        start_to_close = getattr(task, 'swf_start_to_close_timeout', None)
        if start_to_close is None:
            start_to_close = 'NONE'
        else:
            start_to_close = long(start_to_close)
        heartbeat = getattr(task, 'swf_heartbeat_timeout', None)
        if heartbeat is None:
            heartbeat = 'NONE'
        else:
            heartbeat = long(heartbeat)
        schedule_to_close = 'NONE'
        tasks = {
            task.task_id: {
                'class': fullname(task),
                'task_family': task.task_family,
                'deps': [d.task_id for d in deps],
                'task_list': getattr(task, 'swf_task_list', 'default'),
                'params': json.dumps(get_luigi_params(task),
                                     default=dthandler),
                'retries': getattr(task, 'swf_retries', 0),
                'heartbeat_timeout': heartbeat,
                'start_to_close_timeout': start_to_close,
                'schedule_to_close_timeout': schedule_to_close,
                'is_wrapper': isinstance(task, luigi.WrapperTask),
            }
        }
        for dep in deps:
            tasks.update(self._get_all_tasks(dep))
        return tasks

    def register(self):
        tasks = self._get_all_tasks(self.workflow_task)
        registerables = []
        registerables.append(swf.Domain(name=self.domain))
        task_dats = set((t['task_family'], t['task_list'])
                        for (t_id, t) in iteritems(tasks))
        for task_dat in task_dats:
            registerables.append(swf.ActivityType(domain=self.domain,
                                                  version=self.version,
                                                  name=task_dat[0],
                                                  task_list=task_dat[1]))
        wf_name = self.workflow_task.task_family
        wf_task_list = getattr(self.workflow_task, 'swf_task_list', 'default')
        registerables.append(swf.WorkflowType(domain=self.domain,
                                              version=self.version,
                                              name=wf_name,
                                              task_list=wf_task_list))
        for swf_entity in registerables:
            try:
                swf_entity.register()
                print_(swf_entity.name, 'registered successfully')
            except (SWFDomainAlreadyExistsError, SWFTypeAlreadyExistsError):
                print_(swf_entity.__class__.__name__, swf_entity.name,
                       'already exists')

    def execute(self):
        all_tasks = self._get_all_tasks(self.workflow_task)
        logger.debug('LuigiSwfExecutor().execute(), all_tasks:\n%s',
                     pp.pformat(all_tasks))
        wf_id = self.workflow_task.task_id
        wf_input = pickle.dumps(all_tasks)
        wf_type = swf.WorkflowType(domain=self.domain,
                                   version=self.version,
                                   name=self.workflow_task.task_family,
                                   task_list='luigi')
        timeout = getattr(self.workflow_task, 'swf_wf_start_to_close_timeout')
        timeout = str(int(timeout))
        logger.info('LuigiSwfExecutor().execute(), executing workflow %s',
                    wf_id)
        execution = wf_type.start(workflow_id=wf_id, input=wf_input,
                                  execution_start_to_close_timeout=timeout)
        return execution
