sudo iptables -nvxt filter -L INPUT | head -n 12 | tail -n 10 > record
sudo iptables -nvxt filter -L OUTPUT | head -n 12 | tail -n 10 >> record
awk '{sum += $2};END {print sum}' record
