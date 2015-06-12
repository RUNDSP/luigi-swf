from luigi_swf import decider


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


def test_get_all_schedulings():
    # Setup
    events = fixture_events()
    uut = decider.WfState()

    # Execute
    actual = uut._get_all_schedulings(events)

    # Test
    expected = {'Task3': 1, 'Task1': 1}
    assert actual == expected


def test_get_completed():
    # Setup
    events = fixture_events()
    task_configs = fixture_task_configs()
    uut = decider.WfState()

    # Execute
    actual = uut._get_completed(events, task_configs)

    # Test
    expected = ['Task3', 'Task6', 'Task7']
    assert actual == expected


def test_get_failures():
    # Setup
    events = fixture_events()
    uut = decider.WfState()

    # Execute
    actual = uut._get_failures(events)

    # Test
    expected = {'Task1': 1}
    assert actual == expected


def test_get_runnables():
    # Setup
    state = decider.WfState()
    state.completed = ['Task3']
    state.running = ['Task4']
    task_configs = fixture_task_configs()
    decider.LuigiSwfDecider.__init__ = lambda s: None
    uut = decider.LuigiSwfDecider()

    # Execute
    actual = uut._get_runnables(state, task_configs)

    # Test
    expected = {
        'Task1': {'deps': ['Task3'], 'is_wrapper': False},
        'Task2': {'deps': ['Task3'], 'is_wrapper': False},
        # TODO: does the wrapper task go here?
        'Task6': {'deps': ['Task3'], 'is_wrapper': True},
    }
    assert actual == expected


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
            'eventType': 'ActivityTaskScheduled',
            'activityTaskScheduledEventAttributes': {
                'activityId': 'Task3',
            },
        },
        {
            'eventId': 3,
            'eventType': 'ActivityTaskCompleted',
            'activityTaskCompletedEventAttributes': {
                'scheduledEventId': 2,  # Task3
            },
        },
        {
            'eventId': 4,
            'eventType': 'ActivityTaskScheduled',
            'activityTaskScheduledEventAttributes': {
                'activityId': 'Task1',
            },
        },
        {
            'eventId': 5,
            'eventType': 'ActivityTaskFailed',
            'activityTaskFailedEventAttributes': {
                'scheduledEventId': 4,  # Task1
            },
        },
    ]


def fixture_task_configs():
    return {
        'Task1': {'deps': ['Task3'], 'is_wrapper': False},
        'Task2': {'deps': ['Task3'], 'is_wrapper': False},
        'Task3': {'deps': [], 'is_wrapper': False},
        'Task4': {'deps': [], 'is_wrapper': False},
        'Task5': {'deps': ['Task1'], 'is_wrapper': True},
        'Task6': {'deps': ['Task3'], 'is_wrapper': True},
        'Task7': {'deps': ['Task6'], 'is_wrapper': True},
    }
