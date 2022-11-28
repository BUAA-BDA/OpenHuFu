if [ $# -eq 1 ]
then
  if [ $1 == "TPCHBenchmark" ]
  then
    NAME="TPCHBenchmark" docker-compose up
  elif [ $1 == "TPCHOfficialBenchmark" ]
  then
    NAME="TPCHOfficialBenchmark" docker-compose up
  elif [ $1 == "JoinAndSum" ]
    then
      NAME="JoinAndSum" docker-compose up
  else
    echo "./startTest.sh [TPCHBenchmark|TPCHOfficialBenchmark|JoinAndSum]"
  fi
else
  echo "./startTest.sh [TPCHBenchmark|TPCHOfficialBenchmark|JoinAndSum]"
fi
