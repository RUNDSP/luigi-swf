#!/bin/bash

source /mnt/app/audience/shared/virtualenv/shared/bin/activate
source ~/.bashrc
swf_decider_server.py $1
