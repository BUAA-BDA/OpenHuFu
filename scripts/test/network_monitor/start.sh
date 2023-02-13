start() {
  iptables -I INPUT -p tcp --dport $1
  iptables -I OUTPUT -p tcp --sport $1
}

if [ ! -n "$1" ];then
  echo "Port cannot be empty!"
  echo "You can use the following command to monitor the port: sudo bash start.sh 8888"
  exit
fi

export PORT=$1
echo "Monitoring port: "  ${PORT}
start ${PORT}
