#!/usr/bin/env python

import argparse


if __name__ == '__main__':
    from luigi_swf.log import configure_logging
    files_preserve = configure_logging()
    from luigi_swf.decider import DeciderServer
    parser = argparse.ArgumentParser(description='start/stop SWF decider')
    parser.add_argument('action', choices=['start', 'stop'])
    parser.add_argument('--identity', default=None)
    args = parser.parse_args()
    server = DeciderServer(version='unspecified', identity=args.identity,
                           files_preserve=files_preserve)
    if args.action == 'start':
        server.start()
    elif args.action == 'stop':
        server.stop()
