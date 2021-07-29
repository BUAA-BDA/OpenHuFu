if [ $# -eq 0 ]
then
  echo "usage : ./start.sh [config_path]"
else
  echo "start postgresql federate server with config file $1"
  mvn exec:java -Dexec.mainClass="group.bda.federate.server.PostgresqlServer" -Dexec.args="-c $1"
fi