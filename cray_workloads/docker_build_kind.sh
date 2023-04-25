#!/usr/bin/env bash
set -e

docker build . -t public.ecr.aws/cilantro/cray-workloads
docker push public.ecr.aws/cilantro/cray-workloads:latest
kind load docker-image public.ecr.aws/cilantro/cray-workloads:latest
