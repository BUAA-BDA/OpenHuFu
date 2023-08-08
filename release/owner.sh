#!/bin/bash

#set -e

start() {
  echo "load config from $1"
  echo "load task from $2"
  filename=$(echo $(basename $1) | cut -d . -f1)
  log4j="./config/log4j.properties"
  export OPENHUFU_ROOT=$PWD
  log_file=$(date +%m%d)_$(date +%H%M%S)
  nohup java -Dlog4j.configuration=file:"$log4j" -jar ./bin/owner_server.jar -c $1 -t $2 > ${log_dir}/owner_${filename}_${log_file}.log 2>&1 &
  echo "pid is "$!
  echo $filename $! >> ${log_dir}/owner_pid.txt
  echo $filename" started"
}

stop() {
  cat ${log_dir}/owner_pid.txt | while read line
  do
    owner=${line%% *}
    pid=${line##* }
    echo "stop "$owner", kill "$pid
    kill -9 $pid
  done
  rm ${log_dir}/owner*.log
  rm ${log_dir}/owner_pid.txt
}

usage() {
  echo "usage: ./owner.sh [start <owner_config_path> <task_file_path>] | [stop]"
}

log_dir='./log'

mkdir -p ${log_dir}

if [ $# -eq 0 ]
then
  usage
elif [ $1 == "start" ]
then
  if [ $# -eq 3 ]
  then
    start $2 $3
  else
    echo "no config file or task file"
    usage
  fi
elif [ $1 == "stop" ]
then
  stop
else
  echo "too much arguments"
  usage
fi