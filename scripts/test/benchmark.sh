#!/bin/bash

set -e

export OPENHUFU_ROOT=$PWD

java -jar benchmark/target/benchmarks.jar -c benchmark/src/main/resources/user-configs.json
