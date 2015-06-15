import datetime

import luigi

from luigi_swf import tasks


class MyDependency(luigi.Task):

    dt = luigi.DateParameter()


class MyTask(luigi.Task):

    dt = luigi.DateParameter()

    def requires(self):
        return MyDependency(dt=self.dt)


def test_get_task_configurations():
    # Setup
    t = MyTask(dt=datetime.date(2050, 1, 1))

    # Execute
    all_tasks = tasks.get_task_configurations(t)

    # Test
    expected = {
        'MyDependency(dt=2050-01-01)': {
            'class': ('test_tasks', 'MyDependency'),
            'task_family': 'MyDependency',
            'deps': [],
            'task_list': 'default',
            'params': '{"dt": "2050-01-01"}',
            'retries': tasks.RetryWait(max_failures=0),
            'heartbeat_timeout': 'NONE',
            'start_to_close_timeout': 'NONE',
            'schedule_to_start_timeout': 300,
            'schedule_to_close_timeout': 'NONE',
            'is_wrapper': False,
            'running_mutex': None,
        },
        'MyTask(dt=2050-01-01)': {
            'class': ('test_tasks', 'MyTask'),
            'task_family': 'MyTask',
            'deps': ['MyDependency(dt=2050-01-01)'],
            'task_list': 'default',
            'params': '{"dt": "2050-01-01"}',
            'retries': tasks.RetryWait(max_failures=0),
            'heartbeat_timeout': 'NONE',
            'start_to_close_timeout': 'NONE',
            'schedule_to_start_timeout': 300,
            'schedule_to_close_timeout': 'NONE',
            'is_wrapper': False,
            'running_mutex': None,
        }
    }
    assert all_tasks == expected
