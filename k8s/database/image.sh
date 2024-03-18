#!/bin/bash

set -ex

docker pull postgis/postgis:latest
docker tag postgis/postgis:latest docker.oa.com:5000/mpc/postgis/postgis:1.6
docker build -f ./Dockerfile -t docker.oa.com:5000/mpc/pgvector/pgvector:1.6 .
docker push docker.oa.com:5000/mpc/pgvector/pgvector:1.6
kubectl apply -f ./k8s/database/postgis-storage.yaml
kubectl apply -f ./k8s/database/postgis.yaml