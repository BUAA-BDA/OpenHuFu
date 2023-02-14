#!/bin/bash

set -e

if [ $# -ne 2 ]; then
  echo 'please input 2 params'
  exit
fi

dstDir='./dataset/databases'
databaseNum=$1
dataSize=$2

if [ -d $dstDir ];
then
echo "delete existing dir"
rm -rf $dstDir
fi

mkdir $dstDir

i=0
while ((i < databaseNum))
do
    mkdir $dstDir/database$i
    ((i = i + 1))
done

((totalSize = databaseNum * dataSize))
echo "generating data, total size: $totalSize M"
cd ./dataset/TPC-H\ V3.0.1/dbgen/
./dbgen -f -s $totalSize
cd ../../..
pwd
i=0
while ((i < databaseNum))
do
	echo "separating data, running for database$i"
	((a = i * 150 + 1))
	((b = (i + 1) * 150))
	sed -n "$a,$b"p ./dataset/TPC-H\ V3.0.1/dbgen/customer.tbl | sed 's/.$//' > $dstDir/database$i/customer.tbl

	((a = i * 6000 + 1))
	((b = (i + 1) * 6000))
	sed -n "$a,$b"p ./dataset/TPC-H\ V3.0.1/dbgen/lineitem.tbl | sed 's/.$//' > $dstDir/database$i/lineitem.tbl

	((a = i * 6000 + 1))
	((b = (i + 1) * 6000))
	sed -n "$a,$b"p ./dataset/TPC-H\ V3.0.1/dbgen/orders.tbl | sed 's/.$//' > $dstDir/database$i/orders.tbl

	((a = i * 200 + 1))
	((b = (i + 1) * 200))
	sed -n "$a,$b"p ./dataset/TPC-H\ V3.0.1/dbgen/part.tbl | sed 's/.$//' > $dstDir/database$i/part.tbl

	((a = i * 800 + 1))
	((b = (i + 1) * 800))
	sed -n "$a,$b"p ./dataset/TPC-H\ V3.0.1/dbgen/partsupp.tbl | sed 's/.$//' > $dstDir/database$i/partsupp.tbl

	((a = i * 10 + 1))
	((b = (i + 1) * 10))
	sed -n "$a,$b"p ./dataset/TPC-H\ V3.0.1/dbgen/supplier.tbl | sed 's/.$//' > $dstDir/database$i/supplier.tbl

	sed 's/.$//' ./dataset/TPC-H\ V3.0.1/dbgen/nation.tbl > $dstDir/database$i/nation.tbl
	sed 's/.$//' ./dataset/TPC-H\ V3.0.1/dbgen/region.tbl > $dstDir/database$i/region.tbl
	((i = i + 1))
done
rm ./dataset/TPC-H\ V3.0.1/dbgen/*.tbl
