stop() {
  sudo iptables -D INPUT -p tcp --dport $1
  sudo iptables -D OUTPUT -p tcp --sport $1
}

if [ ! -n "$1" ];then
  echo "Port cannot be empty!"
  echo "You can use the following command to stop the monitor: sudo bash stop.sh 8888"
  exit
fi
export PORT=$1
echo "Monitoring port: "  ${PORT}
stop ${PORT}
