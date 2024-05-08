#!/bin/bash

set -e

bash ./owner.sh start ./config/spatial-csv/spatial-csv-owner1.json
bash ./owner.sh start ./config/spatial-csv/spatial-csv-owner2.json
bash ./owner.sh start ./config/spatial-csv/spatial-csv-owner3.json