luigi-swf
=========

[![Circle CI](https://circleci.com/gh/RUNDSP/luigi-swf.svg?style=svg)](https://circleci.com/gh/RUNDSP/luigi-swf)

[Spotify's Luigi](https://github.com/spotify/luigi) + [Amazon's Simple Workflow Service (SWF)](http://aws.amazon.com/swf/) integration

Runs Luigi workflows across many worker processes on different nodes. Each
worker process can select a "task list" (a string), which allows you to specify
which nodes a task can run on. SWF provides Amazon-managed advantages such as
workflow state persistence, workflow execution history audit,
failure/timeout/hasn't completed monitoring via CloudWatch, and historical run
time graphing per-task and per-workflow via CloudWatch.

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
