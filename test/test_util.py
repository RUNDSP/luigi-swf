import datetime

import luigi

from luigi_swf import util


class MyTask(luigi.Task):

    dt = luigi.DateParameter()


def test_fullname():
    # Setup
    t = MyTask(dt=datetime.datetime(2050, 1, 1))

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
    t = MyTask(dt=datetime.datetime(2050, 1, 1))

    # Execute
    params = util.get_luigi_params(t)

    # Test
    assert params == {'dt': datetime.datetime(2050, 1, 1)}
