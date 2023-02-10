#!/bin/bash

set -e

bash ./build/script/extract_tpc_h.sh

java -jar benchmark/target/benchmarks.jar $1 -rf json
