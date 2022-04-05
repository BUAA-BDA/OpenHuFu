mvn clean
mvn install -T 0.5C -Dmaven.test.skip=true
mkdir -p ./release/bin
cp client/target/*-with-dependencies.jar ./release/bin/onedb_client.jar
cp server/target/*-with-dependencies.jar ./release/bin/onedb_server.jar
# cp backend/target/backend-*.jar ./release/bin/backend.jar
