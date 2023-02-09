#!/bin/bash

set -ex

export DATASET_HOME=dataset

tar -xvf ${DATASET_HOME}/TPC-H.tar.gz -C ${DATASET_HOME}