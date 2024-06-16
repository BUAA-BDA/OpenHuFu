start() {
    nohup java -Dlog4j.configurationFile=log4j2.xml -jar -Xms4g -Xmx10g ../../release/bin/postgresql_server.jar -c osm_$1/config$2.json > log$1_$2.txt 2>&1 &
    echo $! >> pid
}

start $1 $2
