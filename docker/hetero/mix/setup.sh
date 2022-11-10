#!/bin/bash

if [ $# -eq 0 ]
then
  echo "./setup.sh [db|owner|user|all]"
else
  if [ $1 == "db" ]
  then
    cd database
    docker-compose up -d
    cnt=0
    until [ "`docker inspect -f {{.State.Health.Status}} htmysql3`" == "healthy" ]; do
      if [ $cnt -eq 20 ]
      then
        echo "mysql timeout"
        exit 1
      fi
      sleep 5;
      ((cnt=cnt+5))
      echo "waiting for database init...($cnt s)"
    done;
  elif [ $1 == "owner" ]
  then
    cd owner
    docker-compose up -d
  elif [ $1 == "user" ]
  then
    cd user
    ./start.sh
  elif [ $1 == "all" ]
  then
    cd database
    docker-compose up -d
    echo "waiting for database init"
    cnt=0
    until [ "`docker inspect -f {{.State.Health.Status}} htmysql3`" == "healthy" ]; do
      if [ $cnt -eq 20 ]
      then
        echo "mysql timeout"
        exit 1
      fi
      sleep 5;
      ((cnt=cnt+5))
      echo "waiting...($cnt s)"
    done;
    cd ../owner
    docker-compose up -d
    cd ../user
    ./start.sh
  fi
fi