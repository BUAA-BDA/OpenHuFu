if [ $# -eq 0 ]
then
  echo "usage : ./start.sh [config_path]"
else
  echo "start geomesa federate server with config file $1"
  mvn exec:java -Dexec.mainClass="group.bda.federate.server.GeomesaServer" -Dexec.args="-c $1"
fi
