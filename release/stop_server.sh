#!/bin/bash
kill -9 $(cat ./log/pid)
echo "stop all"
rm ./log/pid