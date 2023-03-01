#!/bin/bash

set -ex

thread=2C

if [ $# -eq 0 ]; then
  mvn clean install -T ${thread} -Dmaven.test.skip=true
  mkdir -p ./release/bin
  mkdir -p ./release/adapter
  mkdir -p ./release/udf/scalar
  cp owner/target/*-with-dependencies.jar ./release/bin/owner_server.jar
  cp adapter/adapter-csv/target/*-with-dependencies.jar ./release/adapter/adapter_csv.jar
  cp udf/spatial-udf/target/*-with-dependencies.jar ./release/udf/scalar/spatial_udf.jar
elif [ $1 == "owner" ]; then
  mvn install -T ${thread} -Dmaven.test.skip=true -pl $1
  cp owner/target/*-with-dependencies.jar ./release/bin/owner_server.jar
elif [ $1 == "adapter" ]; then
  mvn install -T ${thread} -Dmaven.test.skip=true -pl $1
  cp adapter/adapter-csv/target/*-with-dependencies.jar ./release/adapter/adapter_csv.jar
elif [ $1 == "udf" ]; then
  mvn install -T ${thread} -Dmaven.test.skip=true -pl $1
  cp udf/spatial-udf/target/*-with-dependencies.jar ./release/udf/scalar/spatial_udf.jar
elif [ $1 == "benchmark" ]; then
  mvn install -T ${thread} -Dmaven.test.skip=true -pl $1
else
  echo "try: package.sh [owner|daapter|benchmark]"
fi