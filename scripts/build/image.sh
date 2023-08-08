#!/bin/bash

set -ex

./scripts/build/package.sh
docker pull openjdk:11
docker build -f ./Dockerfile -t openhufu-server:1.0 .