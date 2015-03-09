from luigi_swf import util


class MyClass(object):
    pass


def test_fullname():
    # Setup
    t = MyClass()

    # Execute
    mod, cls = util.fullname(t)

    # Test
    assert mod == 'test_util'
    assert cls == 'MyClass'
