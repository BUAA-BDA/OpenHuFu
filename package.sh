#!/bin/bash

mvn -T 1C install -Dmaven.test.skip=true
mkdir -p ./release/bin
cp hufu-core/target/*-with-dependencies.jar ./release/bin/hufu.jar
cp hufu-postgresql-driver/target/*-with-dependencies.jar ./release/bin/postgresql_server.jar
cp hufu-mysql-driver/target/hufu-mysql-driver-1.0.0.jar ./release/bin/mysql_server.jar
cp hufu-spatialite-driver/target/hufu-spatialite-driver-1.0.0.jar ./release/bin/spatialite_server.jar
cp hufu-evaluation/target/*-with-dependencies.jar ./release/bin/evaluation.jar
#cp hufu-geomesa-driver/target/hufu-geomesa-server-1.0.0.jar ./release/bin/geomesa_server.jar
#cp hufu-simba-driver/target/hufu-simba-server-1.0.0.jar ./release/bin/simba_server.jar
#cp hufu-spatialhadoop-driver/target/hufu-spatialhadoop-server-1.0.0.jar ./release/bin/spatialhadoop_server.jar
