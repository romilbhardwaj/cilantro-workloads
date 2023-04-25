#!/usr/bin/env bash
set -e

python ../drivers/cray_runscript.py \
       --cray-utilfreq 10 \
       --cray-logdir ./testlogs/ \
       --cray-workload-type mltrain_task \
       --ray-svc-name None \
       --train-batch-size 1000 \
       --train-num-iters 100 \
       --sleep-time 0.0 \
       --train-data-path ../../train_data/naval_propulsion.p
