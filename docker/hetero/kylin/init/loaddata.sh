#!/bin/bash

set +e
HTTP_CODE=`curl -k -I -m 10 -o /dev/null -s -w %{http_code} http://localhost:7070/kylin/routes.json`
while [[ ${HTTP_CODE} != 200 ]];do
    sleep 10
    HTTP_CODE=`curl -k -I -m 10 -o /dev/null -s -w %{http_code} http://localhost:7070/kylin/routes.json`
done
set -e
hive -f /opt/hive/init/init.sql

curl -X POST --location "http://localhost:7070/kylin/api/projects" \
    -H "Content-Type: application/json;charset=utf-8" \
    -d "{
          \"projectDescData\": \"{\\\"name\\\":\\\"default\\\",\\\"description\\\":\\\"\\\",\\\"override_kylin_properties\\\":{}}\"
        }" \
    --basic --user ADMIN:KYLIN

curl -X POST --location "http://10.193.113.100:7070/kylin/api/tables/default.student,default.time/default" \
    -H "Content-Type: application/json;charset=utf-8" \
    -d "{
          \"calculate\": false
        }" \
    --basic --user ADMIN:KYLIN