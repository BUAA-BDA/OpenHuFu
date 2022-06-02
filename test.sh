#!/bin/bash

cd docker/test
docker-compose down
cd ../owner
docker-compose down
cd ../database
docker-compose down
docker-compose up -d
cd ../owner
mkdir -p cert
cp -r ../cert/ci/* cert
docker-compose up -d
cd ../test
docker-compose up
sleep 1
docker-compose down
cd ../owner
docker-compose down
cd ../database
docker-compose down
