#!/bin/bash

#set -e

start() {
  echo "load config from $1"
  filename=$(echo $(basename $1) | cut -d . -f1)
  log4j="./config/log4j.properties"
  if [ $# -eq 2 ]
  then
    export DOMAIN_ID=$2
    filename=$filename_$2
  fi
  export OPENHUFU_ROOT=$PWD
  log_file=$(date +%m%d)_$(date +%H%M%S)
  nohup java -Dlog4j.configuration=file:"$log4j" -jar ./bin/user_client.jar -c $1 > ${log_dir}/user_${filename}_${log_file}.log 2>&1 &
  echo "pid is "$!
  echo $filename $! >> ${log_dir}/user_pid.txt
  echo $filename" started"
}

stop() {
  cat ${log_dir}/user_pid.txt | while read line
  do
    user=${line%% *}
    pid=${line##* }
    echo "stop "$user", kill "$pid
    kill -9 $pid
  done
  rm ${log_dir}/user*.log
  rm ${log_dir}/user_pid.txt
}

usage() {
  echo "usage: ./user.sh [start <user_config_path> [<log4j_properties_path>]] | [stop]"
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
      start $2
  fi
elif [ $1 == "stop" ]
then
  stop
else
  echo "too much arguments"
  usage
fi