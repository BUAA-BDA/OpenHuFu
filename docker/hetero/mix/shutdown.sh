#!/bin/bash


if [ $# -eq 0 ]
then
  echo "./setup.sh [db|owner|all]"
else
  if [ $1 == "db" ]
  then
    cd database
    docker-compose down
  elif [ $1 == "owner" ]
  then
    cd owner
    docker-compose down
  elif [ $1 == "all" ]
  then
    cd owner
    docker-compose down
    cd ../database
    docker-compose down
  fi
fi
