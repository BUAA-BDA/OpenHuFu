mvn clean
mvn install -T 0.5C -Dmaven.test.skip=true
mkdir -p ./release/bin
cp user-client/target/*-with-dependencies.jar ./release/bin/onedb_user_client.jar
cp owner-postgresql/target/*-with-dependencies.jar ./release/bin/onedb_postgresql_owner.jar
cp owner-mysql/target/*-with-dependencies.jar ./release/bin/onedb_mysql_owner.jar
# cp backend/target/backend-*.jar ./release/bin/backend.jar
