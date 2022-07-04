#!/bin/bash

if [ $# -eq 1 ]
then
  if [ $1 == "start" ]
  then
    cd database
    docker-compose up -d
    echo "wait for database initialization"
    sleep 5
    cd ../owner
    docker-compose up -d
    cd ..
    docker-compose up -d
  elif [ $1 == "stop" ]
  then
    docker-compose down
    cd ../owner
    docker-compose down
    cd ../database
    docker-compose down
  else
    echo "./benchmark.sh [start|stop]"
  fi
else
  echo "./benchmark.sh [start|stop]"
fi
