mvn clean
mvn install -Dmaven.test.skip=true
mkdir -p ./release/bin
cp client/target/*-with-dependencies.jar ./release/bin/onedb_client.jar
cp server/target/*-with-dependencies.jar ./release/bin/onedb_server.jar