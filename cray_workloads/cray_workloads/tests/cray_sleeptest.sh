#!/usr/bin/env bash
set -e

python ../drivers/cray_runscript.py \
       --cray-utilfreq 10 \
       --cray-logdir ./testlogs/ \
       --cray-workload-type sleep_task \
       --ray-svc-name None \
       --sleep-time 5 \
       --trace-path ../../traces/twit-b1000-n88600.csv \
       --trace-scalefactor 1
