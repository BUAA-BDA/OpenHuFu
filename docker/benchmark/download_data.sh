#!/bin/bash

download() {
  if [ ! -f "$1.tar.gz" ]
  then
    echo "start download 10^$1 scale dataset"
    wget http://hufudb.com/data/$1.tar.gz
  else
    echo "dataset $1.tar.gz already exists"
  fi
  rm -rf database/data
  tar -xvzf $1.tar.gz
  mv $1 database/data
}

if [ $# -eq 1 ]
then
  download $1
else
  echo "please enter the scale of data to be downloaded"
  echo "eg"
  echo "./download_data.sh 3"
fi