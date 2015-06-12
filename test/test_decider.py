from luigi_swf.decider import LuigiSwfDecider


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
    all_tasks = fixture_all_tasks()
    state = {
        'completed': ['Task3'],
        'running': ['Task4'],
    }
    decider = LuigiSwfDecider()

    # Execute
    actual = decider._get_runnables(all_tasks, state)

    # Test
    expected = {
        'Task1': {'deps': ['Task3']},
        'Task2': {'deps': ['Task3']},
    }
    assert actual == expected


def test_get_completed_activities():
    # Setup
    events = fixture_events()

    # Execute
