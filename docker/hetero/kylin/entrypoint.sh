#!/bin/bash

nohup /bin/bash /opt/hive/init/loaddata.sh &
/bin/bash /home/admin/entrypoint.sh

