#!/bin/bash

set -ex

function buildFrontEnd() {
  echo "start build front end..."
  cd webapp
  yarn install
  yarn build
  cd ..
  rm -rf backend/src/main/resources/static
  mkdir -p backend/src/main/resources/static
  mv webapp/dist/* backend/src/main/resources/static/
  echo "build front end finish"
}

function buildCoreModule() {
  echo "start building core modules..."
  mvn clean install -Pdocker -T 0.5C -Dmaven.test.skip=true
  mkdir -p ./release/bin
  mkdir -p ./release/adapter
  mkdir -p ./release/lib
  mkdir -p ./release/udf/scalar
  cp user/target/*-with-dependencies.jar ./release/bin/onedb_user_client.jar
  cp owner/target/*-with-dependencies.jar ./release/bin/onedb_owner_server.jar
  cp udf/spatial-udf/target/*-with-dependencies.jar ./release/udf/scalar/spatial_udf.jar
  cp adapter/adapter-postgresql/target/*-with-dependencies.jar ./release/adapter/adapter_postgresql.jar
  cp adapter/adapter-mysql/target/*-with-dependencies.jar ./release/adapter/adapter_mysql.jar
  cp adapter/adapter-oracle/target/*-with-dependencies.jar ./release/adapter/adapter_oracle.jar
  cp adapter/adapter-sqlite/target/*-with-dependencies.jar ./release/adapter/adapter_sqlite.jar
  cp adapter/adapter-csv/target/*-with-dependencies.jar ./release/adapter/adapter_csv.jar
  cp adapter/adapter-json/target/*-with-dependencies.jar ./release/adapter/adapter_json.jar
  cp adapter/adapter-postgis/target/*-with-dependencies.jar ./release/adapter/adapter_postgis.jar
  cp backend/target/backend*.jar ./release/bin/backend.jar
  echo "build core modules done..."
}

if [ $# -eq 0 ]; then
  buildFrontEnd
  buildCoreModule
else
  if [ $1 == "backend" ]; then
    buildFrontEnd
    mvn install -Pdocker -T 0.5C -Dmaven.test.skip=true -pl $1
    cp backend/target/backend*.jar ./release/bin/backend.jar
  elif [ $1 == "core" ]; then
    buildCoreModule
  elif [ $1 == "benchmark" ]; then
    mvn install -Pdocker -T 0.5C -Dmaven.test.skip=true -pl $1
  elif [ $1 == "user" ]; then
    mvn install -Pdocker -T 0.5C -Dmaven.test.skip=true -pl $1
    cp user/target/*-with-dependencies.jar ./release/bin/onedb_user_client.jar
  elif [ $1 == "owner" ]; then
    mvn install -Pdocker -T 0.5C -Dmaven.test.skip=true -pl $1
    cp owner/target/*-with-dependencies.jar ./release/bin/onedb_owner_server.jar
  elif [ $1 == "adapter" ]; then
    mvn install -Pdocker -T 0.5C -Dmaven.test.skip=true -pl $1
    cp adapter/adapter-postgresql/target/*-with-dependencies.jar ./release/adapter/adapter_postgresql.jar
    cp adapter/adapter-mysql/target/*-with-dependencies.jar ./release/adapter/adapter_mysql.jar
    cp adapter/adapter-sqlite/target/*-with-dependencies.jar ./release/adapter/adapter_sqlite.jar
    cp adapter/adapter-csv/target/*-with-dependencies.jar ./release/adapter/adapter_csv.jar
    cp adapter/adapter-postgis/target/*-with-dependencies.jar ./release/adapter/adapter_postgis.jar
  else
    echo "try: package.sh [core|backend|user|owner]"
  fi
fi
