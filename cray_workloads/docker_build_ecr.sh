#!/usr/bin/env bash
set -e

docker build . -t public.ecr.aws/cilantro/cray-workloads:latest
docker push public.ecr.aws/cilantro/cray-workloads:latest
