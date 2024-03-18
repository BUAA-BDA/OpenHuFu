#!/bin/bash

set -x

update_jobID() {
  sed -i 's/jobID: "\(.*\)"/jobID: "'$1'"/g' ./mpc-chart/values.yaml
}

update_task() {
  sed -i 's/taskName: "\(.*\)"/taskName: "'$1'"/g' ./mpc-chart/values.yaml
}

setup_hufu() {
  helm uninstall hufu-server -n mpc
  helm install hufu-server ./mpc-chart/ -n mpc
}

usage() {
  echo "usage: ./setup.sh <job_id> <task_name>"
}

if [ $# -eq 2 ]
then
  update_jobID $1
  update_task $2
else
  usage
fi
setup_hufu

