# OpenHuFu

OpenHuFu is a multi party computation framework used for data query benchmarks.
It provides flexibility for researchers to quickly implement their algorithms.
With its help, we can quickly take the experiment and get the performance of our algorithms.

## Building Hu-Fu from Source

Prerequistes:

- Linux
- Java 11
- Maven (version at least 3.5.2)

Run the following commands

```cmd
git clone https://github.com/BUAA-BDA/OpenHuFu.git
cd OpenHuFu
./build/script/package.sh
```

Hu-Fu is now installed in `release`

//todo

## Data Generate

1. TCP-H

```cmd
cd dataset/TPC-H V3.0.1/dbgen
cp makefile.suite makefile
make
cd scripts
bash generateData.sh dst x y #其中dst是目标文件夹，x是数据库数量（整数），y是每个数据库的数据量（整数，单位为G）
```

## Running OpenHuFu (demo)

//todo



## Data Query Language

1. Plan
2. DSL(Domain Specific Language): Easy to define and parse, takes time to design
3. Function Call: Hard to describe query
4. SQL

## Query Type

* Filter
* Projection
* Join: equi-join, theta join 
* Cross products
* Aggregate(inc. group-by)
* Limited window aggs
* Distinct
* Sort
* Limit
* Common table expressions
* Spatial Query(TODO):
  * range query
  * knn
  * skyline

## Evaluation Metrics

* Communication Cost
* Running Time
  * Data Access Time
  * Encryption Time
  * Decryption Time
  * Query Time
