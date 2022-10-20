#!/bin/bash
set -e

mkdir -p cert
cp -r ../../docker/cert/local/* cert
cd ../../docker/database
docker-compose down
docker-compose up -d