#!/usr/bin/env bash
set -e

python ../drivers/cray_runscript.py \
       --cray-utilfreq 10 \
       --cray-logdir ./testlogs/ \
       --cray-workload-type db_task \
       --ray-svc-name None \
       --sleep-time 0.0 \
       --query-bin 0 \
       --db-path ../../db_data/tpcds_data/sqlite/tpcds.db \
       --queries-file-path ../../db_data/tpcds_data/queries/processed \
       --query-bins-path ../../db_bins/bins_kk_duplicate.json \
       --trace-path ../../traces/twit-b1000-n88600.csv \
       --trace-scalefactor 1
