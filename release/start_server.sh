#!/bin/bash

start() {
  echo "starting server $1..."
  nohup java -Dlog4j.configuration=file:"./config/log4j.properties" -jar ./bin/onedb_postgresql_owner.jar -c ./config/server$1.json > ./log/$1.log 2>&1 &
  echo $! >> ./log/pid_$1
  echo "postgresql server $1 start"
}

mkdir -p log
if [ $# -eq 0 ]
then
  start "1"
  start "2"
  start "3"
elif [ "$1" == "all" ]
then
  start "1"
  start "2"
  start "3"
else
  start $1
fi