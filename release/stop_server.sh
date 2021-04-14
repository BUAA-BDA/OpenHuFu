#!/bin/bash
stop() {
  kill -9 $(cat ./log/pid_$1)
  echo "stop server $1"
  rm ./log/pid_$1
}

if [ $# -eq 0 ]
then
  stop "a"
  stop "b"
  stop "c"
elif [ "$1" == "all" ]
then
  stop "a"
  stop "b"
  stop "c"
else
  stop $1
fi