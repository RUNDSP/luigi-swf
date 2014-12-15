#!/usr/bin/env python

import argparse
import logging
import os
import re
from subprocess import call

import luigi.configuration

from luigi_swf.worker import WorkerServer


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='start/stop SWF worker(s)')
    parser.add_argument('action', choices=['start', 'stop'])
    parser.add_argument('--index', '-i', type=int, default=None)
    parser.add_argument('--identity', default=None)
    parser.add_argument('--task-list', default=None)
    args = parser.parse_args()
    config = luigi.configuration.get_config()
    loglevel_name = config.get('logging', 'level')
    loglevel = getattr(logging, loglevel_name.upper())
    if args.action == 'start':
        if args.index is None:
            # Start all
            num_workers = config.getint('swfscheduler', 'num-workers')
            for worker_idx in xrange(num_workers):
                worker_args = [__file__, 'start', '-i', str(worker_idx)]
                if args.identity is not None:
                    worker_args += ['--identity', args.identity]
                if args.task_list is not None:
                    worker_args += ['--task-list', args.task_list]
                call(worker_args)
        else:
            # Start one
            server = WorkerServer(worker_idx=args.index,
                                  identity=args.identity,
                                  version='unspecified',
                                  loglevel=loglevel,
                                  task_list=args.task_list)
            server.start()
    elif args.action == 'stop':
        if args.index is None:
            # Stop all
            pid_dir = config.get('swfscheduler', 'worker-pid-file-dir')
            re_pid = re.compile(r'^swfworker\-([0-9]+)\.pid(\-waiting)?$')
            for pid_file in os.listdir(pid_dir):
                pid_match = re_pid.match(pid_file)
                if pid_match is None:
                    continue
                worker_idx = pid_match.groups()[0]
                server = WorkerServer(worker_idx=worker_idx,
                                      version='unspecified',
                                      loglevel=loglevel)
                server.stop()
        else:
            # Stop one
            server = WorkerServer(worker_idx=args.index, version='unspecified',
                                  loglevel=loglevel)
            server.stop()
