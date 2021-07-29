export SPARK_LOCAL_DIRS=/data/tmp/sparktmp
mvn exec:java -Dexec.mainClass="group.bda.federate.server.SimbaServer" -Dpackaging=jar -Dfile=./simba-assembly-1.0.jar -Dexec.args="-p 55667 -f /data/home/zhengpengfei/federatedb/federatedb-simba-server/gps_fed2_knn.csv -i /data/home/zhengpengfei/federatedb/federatedb-simba-server/index-simba"
