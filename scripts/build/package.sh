#!/bin/bash

set -ex

thread=2C

if [ $# -eq 0 ]; then
  mvn clean install -T ${thread} -Dmaven.test.skip=true
  mkdir -p ./release/bin
  mkdir -p ./release/adapter
  cp owner/target/*-with-dependencies.jar ./release/bin/owner_server.jar
  cp adapter/adapter-csv/target/*-with-dependencies.jar ./release/adapter/adapter_csv.jar
elif [ $1 == "owner" ]; then
  mvn install -T ${thread} -Dmaven.test.skip=true -pl $1
  cp owner/target/*-with-dependencies.jar ./release/bin/onedb_owner_server.jar
elif [ $1 == "adapter" ]; then
  mvn install -T ${thread} -Dmaven.test.skip=true -pl $1
  cp adapter/adapter-csv/target/*-with-dependencies.jar ./release/adapter/adapter_csv.jar
elif [ $1 == "benchmark" ]; then
  mvn install -T ${thread} -Dmaven.test.skip=true -pl $1
else
  echo "try: package.sh [core|backend|user|owner]"
fi