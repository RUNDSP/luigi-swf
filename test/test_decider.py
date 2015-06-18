import datetime
import json
from pprint import pformat

import boto.swf.layer2 as swf

from luigi_swf import decider, tasks, util


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
    assert wf_state.signaled_retries == {'Task6': 1}
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


def test_get_deps_met():
    # Setup
    state = decider.WfState()
    state.completed = ['Task3']
    state.running = ['Task4']
    task_configs = fixture_task_configs()
    decider.LuigiSwfDecider.__init__ = lambda s: None
    uut = decider.LuigiSwfDecider()

    # Execute
    actual = set(uut._get_deps_met(state, task_configs))

    # Test
    # TODO: do wrapper tasks (Task6) go here?
    expected = set(('Task1', 'Task2', 'Task6'))
    assert actual == expected


def test_get_retryables():
    # Setup
    state = fixture_state()
    task_configs = fixture_task_configs()
    task_configs['Task1']['retries'] = tasks.RetryWait(wait=100)
    decider.LuigiSwfDecider.__init__ = lambda s: None
    uut = decider.LuigiSwfDecider()
    now_s = 20.0
    now = datetime.datetime.utcfromtimestamp(now_s)

    # Execute
    actual = uut._get_retryables(state, task_configs, now)

    # Test
    a_retryables, a_waitables, a_unretryables = actual
    e_retryables = []
    assert a_retryables == e_retryables
    e_waitables = {'Task1': 5 + 100 - now_s}
    assert a_waitables == e_waitables
    e_unretryables = []
    assert a_unretryables == e_unretryables


def test_decide():
    # Setup
    state = fixture_state()
    task_configs = fixture_task_configs()
    decisions = swf.Layer1Decisions()
    decider.LuigiSwfDecider.__init__ = lambda s: None
    uut = decider.LuigiSwfDecider()
    now = datetime.datetime.utcfromtimestamp(20.0)

    # Execute
    uut._decide(state, decisions, task_configs, now)

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
                    'class': ('aoeu', 'Task2'), 'params': {}
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
                    'class': ('aoeu', 'Task4'), 'params': {}
                }, default=util.dthandler),
            },
        },
    ]
    assert normalize_decisions(actual) == normalize_decisions(expected)


def test_wait():
    # Setup
    state = fixture_state()
    task_configs = fixture_task_configs()
    task_configs['Task1']['retries'] = tasks.RetryWait(wait=100)
    decisions = swf.Layer1Decisions()
    decider.LuigiSwfDecider.__init__ = lambda s: None
    uut = decider.LuigiSwfDecider()
    now = datetime.datetime.utcfromtimestamp(20.0)

    # Execute
    uut._decide(state, decisions, task_configs, now)

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
                    'class': ('aoeu', 'Task2'), 'params': {}
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
                    'class': ('aoeu', 'Task4'), 'params': {}
                }, default=util.dthandler),
            },
        },
        {
            'decisionType': 'StartTimer',
            'startTimerDecisionAttributes': {
                'control': 'Task1',
                'startToFireTimeout': '86',
                'timerId': 'retry-Task1',
            },
        },
    ]
    assert normalize_decisions(actual) == normalize_decisions(expected)


def fixture_events():
    return [
        {
            'eventId': 1,
            'eventTimestamp': 1.0,
            'eventType': 'WorkflowExecutionStarted',
            'workflowExecutionStartedEventAttributes': {
                'workflowType': {
                    'version': 'version1',
                },
            },
        },
        {
            'eventId': 2,
            'eventTimestamp': 2.0,
            'eventType': 'ActivityTaskScheduled',
            'activityTaskScheduledEventAttributes': {
                'activityId': 'Task3',
            },
        },
        {
            'eventId': 3,
            'eventTimestamp': 3.0,
            'eventType': 'ActivityTaskCompleted',
            'activityTaskCompletedEventAttributes': {
                'scheduledEventId': 2,  # Task3
            },
        },
        {
            'eventId': 4,
            'eventTimestamp': 4.0,
            'eventType': 'ActivityTaskScheduled',
            'activityTaskScheduledEventAttributes': {
                'activityId': 'Task1',
            },
        },
        {
            'eventId': 5,
            'eventTimestamp': 5.0,
            'eventType': 'ActivityTaskFailed',
            'activityTaskFailedEventAttributes': {
                'scheduledEventId': 4,  # Task1
            },
        },
        {
            'eventId': 6,
            'eventTimestamp': 6.0,
            'eventType': 'ActivityTaskScheduled',
            'activityTaskScheduledEventAttributes': {
                'activityId': 'Task1',
            },
        },
        {
            'eventId': 7,
            'eventTimestamp': 7.0,
            'eventType': 'ActivityTaskTimedOut',
            'activityTaskTimedOutEventAttributes': {
                'scheduledEventId': 6,  # Task1
            },
        },
        {
            'eventId': 8,
            'eventTimestamp': 8.0,
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
    no_retry = tasks.RetryWait(max_failures=0)
    return {
        'Task1': {'deps': ['Task3'], 'is_wrapper': False, 'retries': no_retry,
                  'running_mutex': None, 'start_to_close_timeout': 0,
                  'heartbeat_timeout': 0, 'schedule_to_start_timeout': 0,
                  'schedule_to_close_timeout': 0,
                  'task_family': 'Task1', 'task_list': 'default',
                  'class': ('aoeu', 'Task1'), 'params': {}},
        'Task2': {'deps': ['Task3'], 'is_wrapper': False, 'retries': no_retry,
                  'running_mutex': None, 'start_to_close_timeout': 0,
                  'heartbeat_timeout': 0, 'schedule_to_start_timeout': 0,
                  'schedule_to_close_timeout': 0,
                  'task_family': 'Task2', 'task_list': 'default',
                  'class': ('aoeu', 'Task2'), 'params': {}},
        'Task3': {'deps': [], 'is_wrapper': False, 'retries': no_retry,
                  'running_mutex': None, 'start_to_close_timeout': 0,
                  'heartbeat_timeout': 0, 'schedule_to_start_timeout': 0,
                  'schedule_to_close_timeout': 0,
                  'task_family': 'Task3', 'task_list': 'default',
                  'class': ('aoeu', 'Task3'), 'params': {}},
        'Task4': {'deps': [], 'is_wrapper': False, 'retries': no_retry,
                  'running_mutex': None, 'start_to_close_timeout': 0,
                  'heartbeat_timeout': 0, 'schedule_to_start_timeout': 0,
                  'schedule_to_close_timeout': 0,
                  'task_family': 'Task4', 'task_list': 'default',
                  'class': ('aoeu', 'Task4'), 'params': {}},
        'Task5': {'deps': ['Task1'], 'is_wrapper': True, 'retries': no_retry,
                  'running_mutex': None, 'start_to_close_timeout': 0,
                  'heartbeat_timeout': 0, 'schedule_to_start_timeout': 0,
                  'schedule_to_close_timeout': 0,
                  'task_family': 'Task5', 'task_list': 'default',
                  'class': ('aoeu', 'Task5'), 'params': {}},
        'Task6': {'deps': ['Task3'], 'is_wrapper': True, 'retries': no_retry,
                  'running_mutex': None, 'start_to_close_timeout': 0,
                  'heartbeat_timeout': 0, 'schedule_to_start_timeout': 0,
                  'schedule_to_close_timeout': 0,
                  'task_family': 'Task6', 'task_list': 'default',
                  'class': ('aoeu', 'Task6'), 'params': {}},
        'Task7': {'deps': ['Task6'], 'is_wrapper': True, 'retries': no_retry,
                  'running_mutex': None, 'start_to_close_timeout': 0,
                  'schedule_to_start_timeout': 0, 'heartbeat_timeout': 0,
                  'schedule_to_close_timeout': 0,
                  'task_family': 'Task7', 'task_list': 'default',
                  'class': ('aoeu', 'Task7'), 'params': {}},
    }


def fixture_state():
    wf_state = decider.WfState()
    wf_state.version = 'version1'
    wf_state.schedulings = {'Task3': 1, 'Task1': 2}
    wf_state.completed = ['Task3', 'Task6', 'Task7']
    wf_state.failures = {'Task1': 1}
    wf_state.last_fails = {'Task1': datetime.datetime.utcfromtimestamp(5.0)}
    wf_state.timeouts = {'Task1': 1}
    wf_state.signaled_retries = {'Task6': 1}
    wf_state.running = []  # TODO
    wf_state.wf_cancel_req = False
    wf_state.waiting = []  # TODO
    return wf_state


def normalize_decisions(decisions):
    res = []
    for d in sorted(decisions, key=util.dictsortkey):
        d = d.copy()
        if d['decisionType'] == 'ScheduleActivityTask':
            d['scheduleActivityTaskDecisionAttributes'] = \
                d['scheduleActivityTaskDecisionAttributes'].copy()
            d['scheduleActivityTaskDecisionAttributes']['input'] = \
                pformat(
                    json.loads(
                        d['scheduleActivityTaskDecisionAttributes']['input']))
        res.append(d)
    return res
