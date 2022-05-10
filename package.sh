#!/bin/bash

if [ $# -eq 0 ]
then
  mvn clean install -T 0.5C -Dmaven.test.skip=true
  mkdir -p ./release/bin
  mkdir -p ./release/adapter
  cp user-client/target/*-with-dependencies.jar ./release/bin/onedb_user_client.jar
  cp owner-postgresql/target/*-with-dependencies.jar ./release/bin/onedb_postgresql_owner.jar
  cp owner-mysql/target/*-with-dependencies.jar ./release/bin/onedb_mysql_owner.jar
  cp adapter-postgresql/target/*-with-dependencies.jar ./release/adapter/adapter-postgresql.jar
else
  mvn install -T 0.5C -Dmaven.test.skip=true -pl $1
  if [ $1 -eq "user-client" ]
  then
    cp user-client/target/*-with-dependencies.jar ./release/bin/onedb_user_client.jar
  elif [ $1 -eq "adapter-postgresql" ]
  then
    cp owner-postgresql/target/*-with-dependencies.jar ./release/bin/onedb_postgresql_owner.jar
  fi
fi
