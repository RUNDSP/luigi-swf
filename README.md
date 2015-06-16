luigi-swf
=========

[![Circle CI](https://circleci.com/gh/RUNDSP/luigi-swf.svg?style=svg)](https://circleci.com/gh/RUNDSP/luigi-swf)

[Spotify's Luigi](https://github.com/spotify/luigi) + [Amazon's Simple Workflow Service (SWF)](http://aws.amazon.com/swf/) integration

## Features

### Farm-out to workers on multiple machines.

Specify on which set of nodes (or worker daemons) to run a task via the
`swf_task_list` task attribute. This allows you to easily use separate
resources per-task within the same workflow execution.

### CloudWatch monitoring

SWF task/workflow execution failures, timeouts, and completions are available
as CloudWatch metrics. This package provides a mechanism for automating
the creation and updating of CloudWatch alarms on those metrics to get
SNS alerts for task/worflow failures, timeouts, and has-not-completed-recently.
See the task_basic.py example and `luigi_swf.cw` module for more details.

### Per-task retry configuration

Via the `swf_retries` task attribute. See the task_basic.py example.
Supported strategies are waiting a fixed or exponential amount of time.
You can also add a retry at run-time by signaling the workflow execution
with action "retry" and put the task ID in the message section.

### Running-mutex

Via the `swf_running_mutex` attribute. This allows you to specify that some
tasks should not be run in parallel together. This is only effective per
workflow execution (each workflow execution gets its own mutex). We use this
to reduce contention between heavy Redshift queries in the same workflow.

### SWF integration

SWF provides Amazon-managed advantages such as
workflow state persistence, workflow execution history audit,
failure/timeout/hasn't completed monitoring via CloudWatch, and historical run
time graphing per-task and per-workflow via CloudWatch. Like Luigi's central
planner scheduler, error tracebacks are displayed in SWF's interface.
Additionally, the SWF interface provides buttons for cancelling and retrying
workflow executions.

## Examples

See `./luigi_swf/examples/` for example tasks that make use of SWF's features
like retries and timeouts. See `./luigi_swf/examples/daemons/` for an example
monitrc and deploy strategy to run the daemons (one decider and many
workers).

## Documentation

We don't have documentation yet. See the docstrings or build the API docs
with Sphinx.

## Installation

```bash
pip install luigi-swf
```

## Changelog

See [Releases on GitHub](https://github.com/RUNDSP/luigi-swf/releases)
