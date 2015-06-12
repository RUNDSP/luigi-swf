from luigi_swf import decider


def fixture_events():
    pass


def fixture_all_tasks():
    return {
        'Task1': {'deps': ['Task3']},
        'Task2': {'deps': ['Task3']},
        'Task3': {'deps': []},
        'Task4': {'deps': []},
    }


def test_get_runnables():
    # Setup
    state = decider.WfState()
    state.all_tasks = fixture_all_tasks()
    state.completed = ['Task3']
    state.running = ['Task4']
    uut = decider.LuigiSwfDecider()

    # Execute
    actual = uut._get_runnables(state)

    # Test
    expected = {
        'Task1': {'deps': ['Task3']},
        'Task2': {'deps': ['Task3']},
    }
    assert actual == expected
