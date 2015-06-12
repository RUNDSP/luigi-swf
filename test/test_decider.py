from luigi_swf import decider


def fixture_events():
    pass


def fixture_task_configs():
    return {
        'Task1': {'deps': ['Task3']},
        'Task2': {'deps': ['Task3']},
        'Task3': {'deps': []},
        'Task4': {'deps': []},
        'Task5': {'deps': ['Task1']},
    }


def test_get_runnables():
    # Setup
    state = decider.WfState()
    state.completed = ['Task3']
    state.running = ['Task4']
    task_configs = fixture_task_configs()
    uut = decider.LuigiSwfDecider()

    # Execute
    actual = uut._get_runnables(state, task_configs)

    # Test
    expected = {
        'Task1': {'deps': ['Task3']},
        'Task2': {'deps': ['Task3']},
    }
    assert actual == expected
