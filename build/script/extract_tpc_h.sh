#!/bin/bash

set -e

export DATASET_HOME=dataset
export TPC_FILE=TPC-H\ V3.0.1

if [[ -d ${DATASET_HOME}/${TPC_FILE} ]];then
  echo ${DATASET_HOME}/${TPC_FILE} "already exists!"
  echo "skip extracting TPC-H files!"
else
  echo "extract TPC-H files!"
  tar -xvf ${DATASET_HOME}/TPC-H.tar.gz -C ${DATASET_HOME}
  echo "extract TPC-H files complete!"
fi