#!/usr/bin/env bash
python wrk_runscript.py --wrk-logdir=/tmp/wrk/ \
    --wrk-executable-path='sleep 30 && echo' \
    --wrk-duration=31 \
    --wrk-url=http://frontend.default.svc.cluster.local:5000 \
    --wrk-script=wrk_script.lua
