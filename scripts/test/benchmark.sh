#!/bin/bash

set -e

bash ./scripts/test/extract_tpc_h.sh

java -jar benchmark/target/benchmarks.jar $1 -rf json
