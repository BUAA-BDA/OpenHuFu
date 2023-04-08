#!/bin/bash

set -e

export OPENHUFU_ROOT=$PWD

java -jar ./bin/benchmark.jar -c ./config/user-configs.json
