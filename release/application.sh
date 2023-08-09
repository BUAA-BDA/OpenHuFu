#!/bin/bash

#set -e

start() {
  echo "load config from $1"
  echo "load task from $2"
  filename=$(echo $(basename $1) | cut -d . -f1)
  log4j="./config/log4j.properties"
  if [ $# -eq 3 ]
    then
      filename=$filename_$orgDID
    fi

  export OPENHUFU_ROOT=$PWD
  log_file=$(date +%m%d)_$(date +%H%M%S)
  nohup java -Dlog4j.configuration=file:"$log4j" -jar ./bin/application.jar -c $1 -t $2 > ${log_dir}/application_${filename}_${log_file}.log 2>&1 &
  echo "pid is "$!
  echo $filename $! >> ${log_dir}/application_pid.txt
  echo $filename" started"
}

stop() {
  cat ${log_dir}/application_pid.txt | while read line
  do
    application=${line%% *}
    pid=${line##* }
    echo "stop "$application", kill "$pid
    kill -9 $pid
  done
  rm ${log_dir}/application*.log
  rm ${log_dir}/application_pid.txt
}

usage() {
  echo "usage: ./application.sh [start <owner_config_path> <task_file_path>] | [stop]"
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