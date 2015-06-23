#!/usr/bin/env python

import argparse
import os
import re


if __name__ == '__main__':
    from luigi_swf.log import configure_logging
    files_preserve = configure_logging()
    import luigi.configuration
    from luigi_swf.decider import DeciderServer
    parser = argparse.ArgumentParser(description='start/stop SWF decider')
    parser.add_argument('action', choices=['start', 'stop'])
    parser.add_argument('--identity', default=None)
    args = parser.parse_args()
    if args.action == 'start':
        server = DeciderServer(version='unspecified', identity=args.identity,
                               files_preserve=files_preserve)
        server.start()
    elif args.action == 'stop':
        if args.identity is None or args.identity == '':
            # Stop all
            config = luigi.configuration.get_config()
            pid_dir = config.get('swfscheduler', 'decider-pid-file-dir')
            re_pid = re.compile(r'^swfdecider\-(.+)\.pid(\-waiting)?$')
            for pid_file in os.listdir(pid_dir):
                pid_match = re_pid.match(pid_file)
                if pid_match is None:
                    continue
                worker_idx = pid_match.groups()[0]
                server = DeciderServer(worker_idx=worker_idx,
                                       version='unspecified')
                server.stop()
        else:
            server = DeciderServer(
                version='unspecified', identity=args.identity,
                files_preserve=files_preserve)
            server.stop()
