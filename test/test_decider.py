from luigi_swf import decider


def fixture_events():
    return [
        {
            'eventId': 1,
            'eventType': 'WorkflowExecutionStarted',
            'workflowExecutionStartedEventAttributes': {
                'workflowType': {
                    'version': 'version1',
                },
            },
        },
        {
            'eventId': 2,
            'eventType': 'activityTaskStarted',
            'activityTaskScheduledEventAttributes': {
                'activityId': 'Task3',
            }
        },
        {
            'eventId': 3,
            'eventType': 'activityTaskCompleted',
            'activityTaskCompletedEventAttributes': {
                'scheduledEventId': 1,
            }
        }
    ]


def fixture_task_configs():
    return {
        'Task1': {'deps': ['Task3']},
        'Task2': {'deps': ['Task3']},
        'Task3': {'deps': []},
        'Task4': {'deps': []},
        'Task5': {'deps': ['Task1']},
    }


def test_get_version():
    # Setup
    events = fixture_events()
    uut = decider.WfState()

    # Execute
    actual = uut._get_version(events)

    # Test
    expected = 'version1'
    assert actual == expected


def test_get_task_id():
    # Setup
    events = fixture_events()
    uut = decider.WfState()
    event_attributes = {
        'scheduledEventId': 2,
    }

    # Execute
    actual = uut._get_task_id(events, event_attributes)

    # Test
    expected = 'Task3'
    assert actual == expected


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
