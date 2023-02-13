#!/bin/bash

set -ex

iptables -nvxt filter -L INPUT | head -n 12 | tail -n 10 > record
iptables -nvxt filter -L OUTPUT | head -n 12 | tail -n 10 >> record
echo "Communication Cost: "
awk '{sum += $2};END {print sum}' record
