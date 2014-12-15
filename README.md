luigi-swf
=========

[Spotify's Luigi](https://github.com/spotify/luigi) + [Amazon's Simple Workflow Service (SWF)](http://aws.amazon.com/swf/) integration

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

* 0.11.1: add optional --task-list flag when starting workers. This used to be
in the Luigi config file.
* 0.11: register_activity_worker() now takes the full SWF activity object as an
argument (instead of just the activity ID). also, register_activity_worker() is
called before complete().
* 0.10: added optional free-form identity strings to deciders and activity
workers. Activity workers previously provided the worker index as the identity,
but now the worker index is appended to the given identity string.
* 0.9: added "retry" signal (signal name is "retry", input is SWF Activity ID)
