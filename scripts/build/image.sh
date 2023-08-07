#!/bin/bash

set -ex

./scripts/build/package.sh
docker build -f ./docker/Dockerfile -t openhufu-server:1.0 .