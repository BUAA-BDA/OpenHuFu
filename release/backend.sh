start_owner() {
  echo "starting server $1..."
  nohup java -jar ./bin/backend.jar --spring.config.location=./config/server$1.properties > ./log/$1.log &
  echo $! >> ./log/pid_$1
  echo "server$1 backend start"
}

start_user() {
  echo "starting user..."
  nohup java -jar ./bin/backend.jar --spring.config.location=./config/user.properties > ./log/u.log &
  echo $! >> ./log/pid_u
  echo "user backend start"
}

stop() {
  kill -9 $(cat ./log/pid_$1)
  echo "stop $1"
  rm ./log/pid_$1
}

mkdir -p log
if [ $# -eq 0 ]
then
  start_user
elif [ "$1" = "owner" ]
then
  start_owner "3"
  start_owner "2"
  start_owner "1"
elif [ "$1" = "user" ]
then
  start_user
elif [ "$1" = "stop" ]
then
  stop "1"
  stop "2"
  stop "3"
  stop "u"
else
  start_owner $1
fi