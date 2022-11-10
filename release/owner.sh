#!/bin/bash

start() {
  echo "load config from $1"
  log4j="./config/log4j.properties"
  if [ $# -eq 2 ]
  then
    echo "use log4j properties from $2"
    log4j=$2
  fi
  export ONEDB_ROOT=$PWD
  log_file=$(date +%m%d)_$(date +%H%M%S)
  nohup java -Dlog4j.configuration=file:"$log4j" -jar ./bin/onedb_owner_server.jar -c $1 > ./log/$log_file.log 2>&1 &
  echo $! >> ./log/.pid
  echo "owner start"
}

stop() {
  kill -9 $(cat ./log/.pid)
  echo "stop owner"
  rm ./log/.pid
}

usage() {
  echo "usage: ./owner.sh [start <owner_config_path> [<log4j_properties_path>]] | [stop]"
}

mkdir -p log

if [ $# -eq 0 ]
then
  usage
elif [ $1 == "start" ]
then
  if [ $# -eq 2 ]
  then
    start $2
  elif [ $# -eq 3 ]
  then
    start $2 $3
  else
    echo "no config file"
    usage
  fi
elif [ $1 == "stop" ]
then
  stop
else
  echo "too much arguments"
  usage
fi