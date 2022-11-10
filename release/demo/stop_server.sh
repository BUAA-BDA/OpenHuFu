set -e

stop() {
  kill -9 $(cat log/pid_$1)
  echo "stop server $1"
  rm log/$1.log
  rm log/pid_$1
}
if [ $# -eq 0 ]
then
  stop "1"
  stop "2"
  stop "3"
elif [ "$1" == "all" ]
then
  stop "1"
  stop "2"
  stop "3"
else
  stop $1
fi
