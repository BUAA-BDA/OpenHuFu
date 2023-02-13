start() {
  sudo iptables -I INPUT -p tcp --dport $1
  sudo iptables -I OUTPUT -p tcp --sport $1
}

start 12345
start 12346
start 12347
start 12348
start 12349
start 12350
start 12351
start 12352
start 12353
start 12354
