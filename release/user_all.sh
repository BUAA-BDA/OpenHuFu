#!/bin/bash

set -e


./user.sh start ./config/tasks-KNN.json computer1
./user.sh start ./config/tasks-KNN.json computer2
./user.sh start ./config/tasks-KNN.json computer3