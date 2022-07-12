#!/bin/bash

clean() {
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

clean_ci() {
  cd docker/test
  docker-compose down
  cd ../owner
  docker-compose down
  cd ../database
  docker-compose down
  cd ../hetero/mix
  sh shutdown.sh all
  cd ../../..
}

setup() {
  cd docker/hetero/mix
  ./setup.sh db
  cd ../../database
  docker-compose up -d
  cd ../owner
  mkdir -p cert
  cp -r ../cert/ci/* cert
  docker-compose up -d
  cd ../hetero/mix
  ./setup.sh owner
  cd ../../..
}

setup_ci() {
  cd docker/hetero/mix
  sh setup.sh db
  cd ../../database
  docker-compose up -d
  cd ../owner
  mkdir -p cert
  cp -r ../cert/ci/* cert
  docker-compose up -d
  cd ../hetero/mix
  sh setup.sh owner
  cd ../../..
}

test_ci() {
  cd docker/test
  docker-compose up | grep "BUILD FAILURE" && touch failed
  if [ -e failed ]; then
    docker-compose logs;
    rm failed;
    exist 1;
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
  clean_ci
  setup_ci
  test_ci
  clean_ci
elif [ $1 == "clean" ]
then
  clean
fi