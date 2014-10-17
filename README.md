luigi-swf
=========

Spotify's Luigi + Amazon's Simple Workflow Service (SWF) integration

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

* 0.9: added "retry" signal (signal name is "retry", input is SWF Activity ID)
