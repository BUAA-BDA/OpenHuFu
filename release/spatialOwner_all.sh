#!/bin/bash

set -e

bash ./owner.sh start ./config/spatialOwner1.json  ./config/tasks-KNN.json
bash ./owner.sh start ./config/spatialOwner2.json  ./config/tasks-KNN.json
bash ./owner.sh start ./config/spatialOwner3.json  ./config/tasks-KNN.json