#!/bin/bash

set -ex

clean() {
  cd docker/test
  docker-compose down
  cd ../owner
  docker-compose down
  cd ../database
  docker-compose down
  cd ../hetero/mix
  ./shutdown.sh db
  cd ../../..
}

clean_ci() {
  cd docker/test
  docker-compose down
  cd ../owner
  docker-compose down
  cd ../database
  docker-compose down
  cd ../hetero/mix
  ./shutdown.sh all
  cd ../../..
}

setup() {
  cd docker/hetero/mix
  ./setup.sh db
  cd ../../database
  docker-compose up -d
  sleep 3
  cd ../owner
  mkdir -p cert
  cp -r ../cert/ci/* cert
  docker-compose up -d
  cd ../..
}

setup_ci() {
  cd docker/hetero/mix
  ./setup.sh db
  cd ../../database
  docker-compose up -d
  sleep 3
  cd ../owner
  mkdir -p cert
  cp -r ../cert/ci/* cert
  docker-compose up -d
  cd ../..
}

test_ci() {
  cd docker/test
  docker-compose up | grep "BUILD FAILURE" && touch failed
  if [ -e failed ]; then
    docker-compose logs;
    rm failed;
    exit 1;
  else
    echo "TEST SUCCESS"
  fi
  cd ../..
}

test_local() {
  cd docker/test
  docker-compose up
  cd ../..
}

if [ $# -eq 0 ]
then
  clean
  setup
  test_local
  clean
elif [ $1 == "ci" ]
then
  setup_ci
  test_ci
elif [ $1 == "clean" ]
then
  clean
elif [ $1 == "clean_ci" ]
then
  clean_ci
fi