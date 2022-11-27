#!/bin/bash
set -e

export ONEDB_ROOT=$PWD/..

if [ $# -eq 0 ]; then
  java -Dlog4j.configuration=file:"../config/log4j.properties" -jar ../bin/onedb_user_client.jar -c ../config/client_model.json
elif [ $1 == "benchmark" ]; then
  java -Dlog4j.configuration=file:"../config/log4j.properties" -jar ../bin/onedb_user_client.jar -c ../config/benchmark_client_model.json
else
  echo "use \"benchmark\" for benchmark linkage"
fi