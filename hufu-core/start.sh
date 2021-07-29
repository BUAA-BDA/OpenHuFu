if [ $# -eq 0 ]
then
  echo "start hufu with default setting"
  echo "./start.sh [model_path]"
  mvn exec:java -Dexec.mainClass="group.bda.federate.cmd.HufuCLI" -Dexec.args="-m src/main/resources/model.json"
else
  echo "start hufu with $1"
  mvn exec:java -Dexec.mainClass="group.bda.federate.cmd.HufuCLI" -Dexec.args="-m $1"
fi
