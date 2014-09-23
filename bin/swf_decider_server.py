#!/usr/bin/env python

import argparse
import logging

import luigi.configuration

from luigi_swf.decider import DeciderServer


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='start/stop SWF decider')
    parser.add_argument('action', choices=['start', 'stop'])
    args = parser.parse_args()
    config = luigi.configuration.get_config()
    loglevel_name = config.get('logging', 'level')
    loglevel = getattr(logging, loglevel_name.upper())
    server = DeciderServer(version='unspecified', loglevel=loglevel)
    if args.action == 'start':
        server.start()
    elif args.action == 'stop':
        server.stop()
