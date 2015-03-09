import json
import logging
import pprint

import boto.swf.layer2 as swf
from boto.swf.exceptions import SWFTypeAlreadyExistsError, \
    SWFDomainAlreadyExistsError
import luigi
import luigi.configuration
from six import iteritems, print_

from .util import fullname, get_all_tasks, get_luigi_params, dthandler


logger = logging.getLogger(__name__)
pp = pprint.PrettyPrinter(indent=2)


class LuigiSwfExecutor(object):
    """Workflow execution launcher

    Can receive AWS credentials in ``__init__()`` or read
    ``[swfscheduler]->aws_access_key_id`` and
    ``[swfscheduler]->aws_secret_access_key`` from Luigi's client.cfg.
    Otherwise, boto will try to read the credentials from environment
    variables or the EC2 instance metadata (if using an IAM role).

    :param domain: SWF domain
    :type domain: str
    :param version: SWF version (you may put "unspecified" if you don't
                    need this)
    :type version: str
    :param workflow_task: wrapper task that defines the workflow through
                          its ``requires()``
    :type workflow_task: :class:`luigi.task.WrapperTask`
    :param aws_access_key_id: optional if using environment, config, or IAM
    :type aws_access_key_id: str
    :param aws_secret_access_key: optional if using environment, config, or IAM
    :type aws_secret_access_key: str
    """

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

    def register(self):
        """Registers the workflow type and task types with SWF

        It is necessary to do this each time a new task is added to a workflow.
        It is safest to run this before each call to :meth:`execute` if you are
        just launching a workflow from a cron. However, if you are launching
        many workflows and calling :meth:`execute` many times, you may want to
        consider calling this method only when necessary because it can
        contribute to an SWF API throttling issue.
        """
        tasks = get_all_tasks(self.workflow_task)
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
        """Initiates a workflow execution on SWF and returns immediately

        Run :meth:`register` first.
        """
        wf_id = self.workflow_task.task_id
        wf_input = json.dumps({
            'wf_task': fullname(self.workflow_task),
            'wf_params': get_luigi_params(self.workflow_task),
        }, default=dthandler)
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
