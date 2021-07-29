start() {
    nohup java -Dlog4j.configurationFile=log4j2.xml -jar ../../release/bin/postgresql_server.jar -c config$1.json > log$1.txt 2>&1 &
    echo $! >> pid
}

cat /dev/null > pid
for i in $*
do
    start $i
done