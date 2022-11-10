#!/bin/bash

hdfs dfsadmin -safemode leave
hive -f /opt/hive/init/init.sql
# nohup hive --service metastore  2>&1 &
hive --service metastore &
hive --service hiveserver2 &
while true; do sleep 1000; done
