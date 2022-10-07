#!/bin/bash

if [ $# -eq 0 ]
then
  mvn clean install -T 0.5C -Dmaven.test.skip=true
  mkdir -p ./release/bin
  mkdir -p ./release/adapter
  mkdir -p ./release/lib
  mkdir -p ./release/udf/scalar
  cp user/target/*-with-dependencies.jar ./release/bin/onedb_user_client.jar
  cp owner/target/*-with-dependencies.jar ./release/bin/onedb_owner_server.jar
  cp udf/spatial-udf/target/*-with-dependencies.jar ./release/udf/scalar/spatial_udf.jar
  cp adapter/adapter-postgresql/target/*-with-dependencies.jar ./release/adapter/adapter_postgresql.jar
  cp adapter/adapter-mysql/target/*-with-dependencies.jar ./release/adapter/adapter_mysql.jar
  cp adapter/adapter-sqlite/target/*-with-dependencies.jar ./release/adapter/adapter_sqlite.jar
  cp adapter/adapter-csv/target/*-with-dependencies.jar ./release/adapter/adapter_csv.jar
  cp adapter/adapter-json/target/*-with-dependencies.jar ./release/adapter/adapter_json.jar
  cp adapter/adapter-postgis/target/*-with-dependencies.jar ./release/adapter/adapter_postgis.jar
  cp backend/target/backend*.jar ./release/bin/backend.jar
else
  mvn install -T 0.5C -Dmaven.test.skip=true -pl $1
  if [ $1 == "user" ]
  then
    cp user/target/*-with-dependencies.jar ./release/bin/onedb_user_client.jar
  elif [ $1 == "owner" ]
  then
    cp owner/target/*-with-dependencies.jar ./release/bin/onedb_owner_server.jar
  elif [ $1 == "adapter-postgresql" ]
  then
    cp adapter/adapter-postgresql/target/*-with-dependencies.jar ./release/adapter/adapter_postgresql.jar
  elif [ $1 == "adapter-mysql" ]
  then
    cp adapter/adapter-mysql/target/*-with-dependencies.jar ./release/adapter/adapter_mysql.jar
  elif [ $1 == "adapter-sqlite" ]
  then
    cp adapter/adapter-sqlite/target/*-with-dependencies.jar ./release/adapter/adapter_sqlite.jar
  elif [ $1 == "adapter-csv" ]
  then
    cp adapter/adapter-csv/target/*-with-dependencies.jar ./release/adapter/adapter_csv.jar
  elif [ $1 == "adapter-postgis" ]
  then
    cp adapter/adapter-postgis/target/*-with-dependencies.jar ./release/adapter/adapter_postgis.jar
  elif [ $1 == "backend" ]
  then
    cp backend/target/backend*.jar ./release/bin/backend.jar
  fi
fi
