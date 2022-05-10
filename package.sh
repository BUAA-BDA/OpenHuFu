#!/bin/bash

if [ $# -eq 0 ]
then
  mvn clean install -T 0.5C -Dmaven.test.skip=true
  mkdir -p ./release/bin
  mkdir -p ./release/adapter
  cp user-client/target/*-with-dependencies.jar ./release/bin/onedb_user_client.jar
  cp owner-core/target/*-with-dependencies.jar ./release/bin/onedb_owner_server.jar
  cp adapter-postgresql/target/*-with-dependencies.jar ./release/adapter/adapter_postgresql.jar
  cp adapter-mysql/target/*-with-dependencies.jar ./release/adapter/adapter_mysql.jar
else
  mvn install -T 0.5C -Dmaven.test.skip=true -pl $1
  if [ $1 == "user-client" ]
  then
    cp user-client/target/*-with-dependencies.jar ./release/bin/onedb_user_client.jar
  elif [ $1 == "owner-core" ]
  then
    cp owner-core/target/*-with-dependencies.jar ./release/bin/onedb_owner_server.jar
  elif [ $1 == "adapter-postgresql" ]
  then
    cp adapter-postgresql/target/*-with-dependencies.jar ./release/adapter/adapter-postgresql.jar
  elif [ $1 == "adapter-mysql" ]
  then
    cp adapter-mysql/target/*-with-dependencies.jar ./release/adapter/adapter-mysql.jar
  fi
fi
