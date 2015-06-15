import json

import boto.swf.layer2 as swf

from luigi_swf import decider, util


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


def test_read_wf_state():
    # Setup
    events = fixture_events()
    task_configs = fixture_task_configs()
    wf_state = decider.WfState()

    # Execute
    wf_state.read_wf_state(events, task_configs)

    # Test
    assert wf_state.version == 'version1'
    assert wf_state.schedulings == {'Task3': 1, 'Task1': 2}
    assert wf_state.completed == ['Task3', 'Task6', 'Task7']
    assert wf_state.failures == {'Task1': 1}
    assert wf_state.timeouts == {'Task1': 1}
    assert wf_state.retries == {'Task6': 1}
    assert wf_state.running == []  # TODO: test running count
    assert wf_state.wf_cancel_req is False


def test_get_wf_cancel_requested():
    # Setup
    events = fixture_events_cancel_requested()
    uut = decider.WfState()

    # Execute
    actual = uut._get_wf_cancel_requested(events)

    # Test
    expected = True
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
        'Task1': task_configs['Task1'],
        'Task2': task_configs['Task2'],
        # TODO: does the wrapper task go here?
        'Task6': task_configs['Task6'],
    }
    assert actual == expected


def test_decide():
    # Setup
    state = fixture_state()
    task_configs = fixture_task_configs()
    decisions = swf.Layer1Decisions()
    decider.LuigiSwfDecider.__init__ = lambda s: None
    uut = decider.LuigiSwfDecider()

    # Execute
    uut._decide(state, decisions, task_configs)

    # Test
    actual = decisions._data
    expected = [
        {
            'decisionType': 'ScheduleActivityTask',
            'scheduleActivityTaskDecisionAttributes': {
                'activityId': 'Task2',
                'activityType': {'name': 'Task2', 'version': 'version1'},
                'taskList': {'name': 'default'},
                'scheduleToCloseTimeout': '0',
                'scheduleToStartTimeout': '0',
                'startToCloseTimeout': '0',
                'heartbeatTimeout': '0',
                'input': json.dumps({
                    "running_mutex": None,
                    "schedule_to_start_timeout": 0, "deps": ["Task3"],
                    "schedule_to_close_timeout": 0, "retries": 0,
                    "is_wrapper": False, "start_to_close_timeout": 0,
                    "heartbeat_timeout": 0, "task_list": "default",
                    "task_family": "Task2"
                }, default=util.dthandler),
            },
        },
        {
            'decisionType': 'ScheduleActivityTask',
            'scheduleActivityTaskDecisionAttributes': {
                'activityId': 'Task4',
                'activityType': {'name': 'Task4', 'version': 'version1'},
                'taskList': {'name': 'default'},
                'scheduleToCloseTimeout': '0',
                'scheduleToStartTimeout': '0',
                'startToCloseTimeout': '0',
                'heartbeatTimeout': '0',
                'input': json.dumps({
                    "running_mutex": None,
                    "schedule_to_start_timeout": 0, "deps": [],
                    "schedule_to_close_timeout": 0, "retries": 0,
                    "is_wrapper": False, "start_to_close_timeout": 0,
                    "heartbeat_timeout": 0, "task_list": "default",
                    "task_family": "Task4"
                }, default=util.dthandler),
            },
        },
    ]
    assert normalize_decisions(actual) == normalize_decisions(expected)


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
        {
            'eventId': 6,
            'eventType': 'ActivityTaskScheduled',
            'activityTaskScheduledEventAttributes': {
                'activityId': 'Task1',
            },
        },
        {
            'eventId': 7,
            'eventType': 'ActivityTaskTimedOut',
            'activityTaskTimedOutEventAttributes': {
                'scheduledEventId': 6,  # Task1
            },
        },
        {
            'eventId': 8,
            'eventType': 'WorkflowExecutionSignaled',
            'workflowExecutionSignaledEventAttributes': {
                'signalName': 'retry',
                'input': 'Task6',
            },
        },
    ]


def fixture_events_cancel_requested():
    return fixture_events() + [
        {
            'eventId': 9,
            'eventType': 'WorkflowExecutionCancelRequested',
            'workflowExecutionCancelRequestedEventAttributes': {
            },
        },
    ]


def fixture_task_configs():
    return {
        'Task1': {'deps': ['Task3'], 'is_wrapper': False, 'retries': 0,
                  'running_mutex': None, 'start_to_close_timeout': 0,
                  'heartbeat_timeout': 0, 'schedule_to_start_timeout': 0,
                  'schedule_to_close_timeout': 0,
                  'task_family': 'Task1', 'task_list': 'default'},
        'Task2': {'deps': ['Task3'], 'is_wrapper': False, 'retries': 0,
                  'running_mutex': None, 'start_to_close_timeout': 0,
                  'heartbeat_timeout': 0, 'schedule_to_start_timeout': 0,
                  'schedule_to_close_timeout': 0,
                  'task_family': 'Task2', 'task_list': 'default'},
        'Task3': {'deps': [], 'is_wrapper': False, 'retries': 0,
                  'running_mutex': None, 'start_to_close_timeout': 0,
                  'heartbeat_timeout': 0, 'schedule_to_start_timeout': 0,
                  'schedule_to_close_timeout': 0,
                  'task_family': 'Task3', 'task_list': 'default'},
        'Task4': {'deps': [], 'is_wrapper': False, 'retries': 0,
                  'running_mutex': None, 'start_to_close_timeout': 0,
                  'heartbeat_timeout': 0, 'schedule_to_start_timeout': 0,
                  'schedule_to_close_timeout': 0,
                  'task_family': 'Task4', 'task_list': 'default'},
        'Task5': {'deps': ['Task1'], 'is_wrapper': True, 'retries': 0,
                  'running_mutex': None, 'start_to_close_timeout': 0,
                  'heartbeat_timeout': 0, 'schedule_to_start_timeout': 0,
                  'schedule_to_close_timeout': 0,
                  'task_family': 'Task5', 'task_list': 'default'},
        'Task6': {'deps': ['Task3'], 'is_wrapper': True, 'retries': 0,
                  'running_mutex': None, 'start_to_close_timeout': 0,
                  'heartbeat_timeout': 0, 'schedule_to_start_timeout': 0,
                  'schedule_to_close_timeout': 0,
                  'task_family': 'Task6', 'task_list': 'default'},
        'Task7': {'deps': ['Task6'], 'is_wrapper': True, 'retries': 0,
                  'running_mutex': None, 'start_to_close_timeout': 0,
                  'schedule_to_start_timeout': 0, 'heartbeat_timeout': 0,
                  'schedule_to_close_timeout': 0,
                  'task_family': 'Task7', 'task_list': 'default'},
    }


def fixture_state():
    wf_state = decider.WfState()
    wf_state.version = 'version1'
    wf_state.schedulings = {'Task3': 1, 'Task1': 2}
    wf_state.completed = ['Task3', 'Task6', 'Task7']
    wf_state.failures = {'Task1': 1}
    wf_state.timeouts = {'Task1': 1}
    wf_state.retries = {'Task6': 1}
    wf_state.running = []  # TODO
    wf_state.wf_cancel_req = False
    return wf_state


def normalize_decisions(decisions):
    res = []
    for d in sorted(decisions, key=util.dictsortkey):
        d = d.copy()
        if d['decisionType'] == 'ScheduleActivityTask':
            d['scheduleActivityTaskDecisionAttributes'] = \
                d['scheduleActivityTaskDecisionAttributes'].copy()
            d['scheduleActivityTaskDecisionAttributes']['input'] = \
                json.loads(d['scheduleActivityTaskDecisionAttributes']
                           ['input'])
        res.append(d)
    return res
