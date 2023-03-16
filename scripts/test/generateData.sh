#!/bin/bash

set -e

if [ $# -ne 2 ]; then
  echo 'please input 2 params'
  exit
fi

dstDir='./dataset/databases'
databaseNum=$1
dataSize=$2

echo "party: "$databaseNum", each data size: "$dataSize "MB"

if [ -d $dstDir ];
then
echo "deleting existing dir"
rm -rf $dstDir
fi

mkdir $dstDir

i=0
while ((i < databaseNum))
do
    mkdir $dstDir/database$i
    ((i = i + 1))
done

totalSize=$((databaseNum*dataSize))

if [ $totalSize -lt 1000 ];
then
  totalSize=1
else
  totalSize=$((databaseNum*dataSize/1000))
fi
echo "generating data, total size: $totalSize G"
cd ./dataset/TPC-H\ V3.0.1/dbgen/

./dbgen -f -s $totalSize
cd ../../..
pwd
i=0
echo "N_NATIONKEY | N_NAME | N_REGIONKEY | N_COMMENT" > $dstDir/database0/nation.csv
sed 's/.$//' ./dataset/TPC-H\ V3.0.1/dbgen/nation.tbl >> $dstDir/database0/nation.csv
echo "N_NATIONKEY | N_NAME | N_REGIONKEY | N_COMMENT" > $dstDir/database0/nation.scm
echo "LONG | STRING | LONG | STRING" >> $dstDir/database0/nation.scm

echo "R_REGIONKEY | R_NAME | R_COMMENT" > $dstDir/database0/region.csv
sed 's/.$//' ./dataset/TPC-H\ V3.0.1/dbgen/region.tbl >> $dstDir/database0/region.csv
echo "R_REGIONKEY | R_NAME | R_COMMENT" > $dstDir/database0/region.scm
echo "LONG | STRING | STRING" >> $dstDir/database0/region.scm

while ((i < databaseNum))
do
	echo "separating data, running for database$i"
	((a = i * 150 * dataSize + 1))
	((b = (i + 1) * 150 * dataSize))
	echo "C_CUSTKEY | C_NAME | C_ADDRESS | C_NATIONKEY | C_PHONE | C_ACCTBAL | C_MKTSEGMENT | C_COMMENT" > $dstDir/database$i/customer.csv
	sed -n "$a,$b"p ./dataset/TPC-H\ V3.0.1/dbgen/customer.tbl | sed 's/.$//' >> $dstDir/database$i/customer.csv
	echo "C_CUSTKEY | C_NAME | C_ADDRESS | C_NATIONKEY | C_PHONE | C_ACCTBAL | C_MKTSEGMENT | C_COMMENT" > $dstDir/database$i/customer.scm
	echo "LONG | STRING | STRING | LONG | STRING | DOUBLE | STRING | STRING" >> $dstDir/database$i/customer.scm

	((a = i * 6000 * dataSize + 1))
	((b = (i + 1) * 6000 * dataSize))
	echo "L_ORDERKEY | L_PARTKEY | L_SUPPKEY | L_LINENUMBER | L_QUANTITY | L_EXTENDEDPRICE | L_DISCOUNT | L_TAX | L_RETURNFLAG | L_LINESTATUS | L_SHIPDATE | L_COMMITDATE | L_RECEIPTDATE | L_SHIPINSTRUCT | L_SHIPMODE | L_COMMENT" > $dstDir/database$i/lineitem.csv
	sed -n "$a,$b"p ./dataset/TPC-H\ V3.0.1/dbgen/lineitem.tbl | sed 's/.$//' >> $dstDir/database$i/lineitem.csv
	echo "L_ORDERKEY | L_PARTKEY | L_SUPPKEY | L_LINENUMBER | L_QUANTITY | L_EXTENDEDPRICE | L_DISCOUNT | L_TAX | L_RETURNFLAG | L_LINESTATUS | L_SHIPDATE | L_COMMITDATE | L_RECEIPTDATE | L_SHIPINSTRUCT | L_SHIPMODE | L_COMMENT" > $dstDir/database$i/lineitem.scm
	echo "LONG | LONG | LONG | LONG | LONG | DOUBLE | DOUBLE | DOUBLE | STRING | STRING | DATE | DATE | DATE | STRING | STRING | STRING" >> $dstDir/database$i/lineitem.scm

	((a = i * 6000 * dataSize + 1))
	((b = (i + 1) * 6000 * dataSize))
	echo "O_ORDERKEY | O_CUSTKEY | O_ORDERSTATUS | O_TOTALPRICE | O_ORDERDATE | O_ORDER-PRIORITY | O_CLERK | O_SHIP-PRIORITY | O_COMMENT" > $dstDir/database$i/orders.csv
	sed -n "$a,$b"p ./dataset/TPC-H\ V3.0.1/dbgen/orders.tbl | sed 's/.$//' >> $dstDir/database$i/orders.csv
	echo "O_ORDERKEY | O_CUSTKEY | O_ORDERSTATUS | O_TOTALPRICE | O_ORDERDATE | O_ORDER-PRIORITY | O_CLERK | O_SHIP-PRIORITY | O_COMMENT" > $dstDir/database$i/orders.scm
	echo "LONG | LONG | STRING | DOUBLE | DATE | STRING | STRING | INT | STRING" >> $dstDir/database$i/orders.scm

	((a = i * 200 * dataSize + 1))
	((b = (i + 1) * 200 * dataSize))
	echo "P_PARTKEY | P_NAME | P_MFGR | P_BRAND | P_TYPE | P_SIZE | P_CONTAINER | P_RETAILPRICE | P_COMMENT" > $dstDir/database$i/part.csv
	sed -n "$a,$b"p ./dataset/TPC-H\ V3.0.1/dbgen/part.tbl | sed 's/.$//' >> $dstDir/database$i/part.csv
	echo "P_PARTKEY | P_NAME | P_MFGR | P_BRAND | P_TYPE | P_SIZE | P_CONTAINER | P_RETAILPRICE | P_COMMENT" > $dstDir/database$i/part.scm
	echo "LONG | STRING | STRING | STRING | STRING | INT | STRING | DOUBLE | STRING" >> $dstDir/database$i/part.scm

	((a = i * 800 * dataSize + 1))
	((b = (i + 1) * 800 * dataSize))
	echo "PS_PARTKEY | PS_SUPPKEY | PS_AVAILQTY | PS_SUPPLYCOST | PS_COMMENT" > $dstDir/database$i/partsupp.csv
	sed -n "$a,$b"p ./dataset/TPC-H\ V3.0.1/dbgen/partsupp.tbl | sed 's/.$//' >> $dstDir/database$i/partsupp.csv
	echo "PS_PARTKEY | PS_SUPPKEY | PS_AVAILQTY | PS_SUPPLYCOST | PS_COMMENT" > $dstDir/database$i/partsupp.scm
	echo "LONG | LONG | INT | DOUBLE | STRING" >> $dstDir/database$i/partsupp.scm

	((a = i * 10 * dataSize + 1))
	((b = (i + 1) * 10 * dataSize))
	echo "S_SUPPKEY | S_NAME | S_ADDRESS | S_NATIONKEY | S_PHONE | S_ACCTBAL | S_COMMENT" > $dstDir/database$i/supplier.csv
	sed -n "$a,$b"p ./dataset/TPC-H\ V3.0.1/dbgen/supplier.tbl | sed 's/.$//' >> $dstDir/database$i/supplier.csv
	echo "S_SUPPKEY | S_NAME | S_ADDRESS | S_NATIONKEY | S_PHONE | S_ACCTBAL | S_COMMENT" > $dstDir/database$i/supplier.scm
	echo "LONG | STRING | STRING | LONG | STRING | DOUBLE | STRING" >> $dstDir/database$i/supplier.scm

	((i = i + 1))
done
rm ./dataset/TPC-H\ V3.0.1/dbgen/*.tbl
