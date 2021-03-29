#!/bin/bash
mkdir -p log
echo "starting..."
nohup java -jar ./bin/onedb_server.jar -c ./config/server_config.json > ./log/a.log &
echo $! >> ./log/pid
echo "postgresql server A start"