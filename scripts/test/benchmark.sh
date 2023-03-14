#!/bin/bash

set -e

export OPENHUFU_ROOT=$PWD

java -jar benchmark/target/benchmarks.jar -rf json
