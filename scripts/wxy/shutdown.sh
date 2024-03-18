#!/bin/bash

set -x

helm uninstall hufu-server -n mpc
kubectl delete -f ./k8s/database/postgis.yaml
kubectl delete -f ./k8s/database/postgis-storage.yaml
