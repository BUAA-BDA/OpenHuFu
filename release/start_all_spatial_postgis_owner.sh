#!/bin/bash

#simulate 4 owner in 1 postgis

set -e

bash ./owner.sh start ./config/spatial-postgis/spatial-postgis-owner1.json
bash ./owner.sh start ./config/spatial-postgis/spatial-postgis-owner2.json
bash ./owner.sh start ./config/spatial-postgis/spatial-postgis-owner3.json
bash ./owner.sh start ./config/spatial-postgis/spatial-postgis-owner4.json