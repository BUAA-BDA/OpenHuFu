#!/bin/bash
mkdir -p log
echo "starting server A..."
nohup java -jar ./bin/onedb_server.jar -c ./config/server_config_a.json > ./log/a.log &
echo $! >> ./log/pid
echo "postgresql server A start"

echo "starting server B..."
nohup java -jar ./bin/onedb_server.jar -c ./config/server_config_b.json > ./log/b.log &
echo $! >> ./log/pid
echo "postgresql server B start"