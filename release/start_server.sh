#!/bin/bash

start() {
  echo "starting server $1..."
  nohup java -jar ./bin/onedb_server.jar -c ./config/server_config_$1.json > ./log/$1.log &
  echo $! >> ./log/pid_$1
  echo "postgresql server $1 start"
}

mkdir -p log
if [ $# -eq 0 ]
then
  start "a"
  start "b"
  start "c"
elif [ "$1" == "all" ]
then
  start "a"
  start "b"
  start "c"
else
  start $1
fi