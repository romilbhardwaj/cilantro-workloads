#!/usr/bin/env bash
set -e

docker build . -t public.ecr.aws/cilantro/hr-client:latest
docker push public.ecr.aws/cilantro/hr-client:latest
