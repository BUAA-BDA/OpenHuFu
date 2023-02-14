#!/bin/bash

cd $1
i=0
while ((i < $2))
do
    mkdir database$i
    ((i++))
done
