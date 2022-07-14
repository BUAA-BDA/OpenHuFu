#!/bin/bash

start() {
  echo "load config from $1"
  log4j="./config/log4j.properties"
  if [ $# -eq 2 ]
  then
    echo "use log4j properties from $2"
    log4j=$2
  fi
  java -Dlog4j.configuration=file:"$log4j" -jar ./bin/onedb_user_client.jar -m $1
}

usage() {
  echo "usage: ./user.sh <user_config_path> [<log4j_properties_path>]"
}

if [ $# -eq 0 ]
then
  usage
elif [ $# -eq 1 ]
then
  start $1
elif [ $# -eq 2 ]
then
  start $1 $2
else
  echo "too much arguments"
  usage
fi