#!/bin/bash

set -ex

config_modify() {
  sed -i 's/db1.sql/db2.sql/g' ./k8s/database/postgis.yaml
  sed -i 's/computer1/computer2/g' ./mpc-chart/values.yaml
  sed -i 's/192.168.0.101/192.168.0.104/g' ./mpc-chart/values.yaml
  sed -i 's/spatialOwner1.json/spatialOwner2.json/g' ./Dockerfile
}

update_version() {
  sed -i 's/openhufu-server:1.0/openhufu-server:'$1'/g' ./scripts/build/image.sh
  sed -i 's/tag: "1.0"/tag: "'$1'"/g' ./mpc-chart/values.yaml
}

setup_pg() {
  docker pull postgis/postgis:latest
  docker tag postgis/postgis:latest docker.oa.com:5000/mpc/postgis/postgis:1.6
  docker build -f .k8s/database/Dockerfile -t docker.oa.com:5000/mpc/pgvector/pgvector:1.6 .
  docker push docker.oa.com:5000/mpc/pgvector/pgvector:1.6
  kubectl apply -f ./k8s/database/postgis-storage.yaml
  kubectl apply -f ./k8s/database/postgis.yaml
}

prepare_hufu() {
  ./scripts/build/image.sh
}

usage() {
  echo "usage: ./prepare.sh <party_id> <hufu_version>"
}

if [ $# -eq 2 ]
then
  if [ $1 == "2" ]
  then
    config_modify
  fi
  update_version $2
else
  usage
fi
setup_pg
prepare_hufu
