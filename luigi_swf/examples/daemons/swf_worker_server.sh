#!/bin/bash

source /mnt/app/audience/shared/virtualenv/shared/bin/activate
source ~/.bashrc
swf_worker_server.py $1 $2 $3
