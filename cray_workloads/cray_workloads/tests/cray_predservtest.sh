#!/usr/bin/env bash
set -e

python ../drivers/cray_runscript.py \
       --cray-utilfreq 10 \
       --cray-logdir ./testlogs/ \
       --cray-workload-type predserv_task \
       --ray-svc-name None \
       --serve-chunk-size 32 \
       --sleep-time 0.0 \
       --ps-model-path ../../train_data/news_rfr.p \
       --trace-path ../../traces/twit-b1000-n88600.csv \
       --ps-data-path ../../train_data/news_popularity.p \
       --trace-scalefactor 1
