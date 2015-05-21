luigi-swf
=========

[![Circle CI](https://circleci.com/gh/RUNDSP/luigi-swf.svg?style=svg)](https://circleci.com/gh/RUNDSP/luigi-swf)

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

See [Releases on GitHub](./releases)
