import datetime

import luigi

from luigi_swf import util


class MyDependency(luigi.Task):

    dt = luigi.DateParameter()


class MyTask(luigi.Task):

    dt = luigi.DateParameter()

    def requires(self):
        return MyDependency(dt=self.dt)


def test_fullname():
    # Setup
    t = MyTask(dt=datetime.date(2050, 1, 1))

    # Execute
    modname, clsname = util.fullname(t)

    # Test
    assert modname == 'test_util'
    assert clsname == 'MyTask'


def test_get_class():
    # Execute
    cls = util.get_class('test_util', 'MyTask')

    # Test
    assert cls == MyTask


def test_get_luigi_params():
    # Setup
    t = MyTask(dt=datetime.date(2050, 1, 1))

    # Execute
    params = util.get_luigi_params(t)

    # Test
    assert params == {'dt': datetime.date(2050, 1, 1)}


def test_get_all_tasks():
    # Setup
    t = MyTask(dt=datetime.date(2050, 1, 1))

    # Execute
    all_tasks = util.get_all_tasks(t)

    # Test
    expected = {
        'MyDependency(dt=2050-01-01)': {
            'class': ('test_util', 'MyDependency'),
            'task_family': 'MyDependency',
            'deps': [],
            'task_list': 'default',
            'params': '{"dt": "2050-01-01"}',
            'retries': 0,
            'heartbeat_timeout': 'NONE',
            'start_to_close_timeout': 'NONE',
            'schedule_to_start_timeout': 300,
            'schedule_to_close_timeout': 'NONE',
            'is_wrapper': False,
            'running_mutex': None,
        },
        'MyTask(dt=2050-01-01)': {
            'class': ('test_util', 'MyTask'),
            'task_family': 'MyTask',
            'deps': ['MyDependency(dt=2050-01-01)'],
            'task_list': 'default',
            'params': '{"dt": "2050-01-01"}',
            'retries': 0,
            'heartbeat_timeout': 'NONE',
            'start_to_close_timeout': 'NONE',
            'schedule_to_start_timeout': 300,
            'schedule_to_close_timeout': 'NONE',
            'is_wrapper': False,
            'running_mutex': None,
        }
    }
    assert all_tasks == expected


def test_dt_from_iso():
    # Setup
    iso = '2050-01-02'

    # Execute
    dt = util.dt_from_iso(iso)

    # Test
    expected = datetime.date(2050, 1, 2)
    assert dt == expected
