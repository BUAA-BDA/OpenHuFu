kill -9 $(cat pid)
echo "stop all"
cat /dev/null > pid
rm -rf log*.txt