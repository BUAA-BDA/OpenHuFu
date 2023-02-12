stop() {
  sudo iptables -D INPUT -p tcp --dport $1
  sudo iptables -D OUTPUT -p tcp --sport $1
}

stop 12345
stop 12346
stop 12347
stop 12348
stop 12349
stop 12350
stop 12351
stop 12352
stop 12353
stop 12354
