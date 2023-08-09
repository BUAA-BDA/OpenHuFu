#!/bin/bash

set -e


./application.sh start ./config/spatialOwner1.json ./config/tasks-KNN.json computer1
./application.sh start ./config/spatialOwner2.json ./config/tasks-KNN.json computer2
./application.sh start ./config/spatialOwner3.json ./config/tasks-KNN.json computer3