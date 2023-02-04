# OpenHuFu

OpenHuFu is a multi party computation framework used for data query benchmarks.
It provides flexibility for researchers to quickly implement their algorithms, such as secret sharing, garbled circuit and oblivious transfer.
With its help, we can quickly take the experiment and get the performance of our algorithms.

## Building OpenHuFu from Source

prerequisites:

- Linux
- Java 11
- Maven (version at least 3.5.2)

Run the following commands

```cmd
git clone https://github.com/BUAA-BDA/OpenHuFu.git
cd OpenHuFu
./build/script/package.sh
```

OpenHuFu is now installed in `release`


## Data Generation

[TCP-H](https://www.tpc.org/tpch/)
### How to use it
```cmd
cd dataset/TPC-H V3.0.1/dbgen
cp makefile.suite makefile
make
cd scripts
# dst is the target folder, x is the number of database，y is the volume of each database
bash generateData.sh dst x y
```

## Configuration File
### OwnerSide

### UserSide


## Running OpenHuFu




## Data Query Language

1. Plan
2. Function Call: Hard to describe query

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
* Spatial Queries:
  * range query
  * range counting
  * knn query
  * distance join
  * knn join

## Evaluation Metrics

* Communication Cost
* Running Time
  * Data Access Time
  * Encryption Time
  * Decryption Time
  * Query Time
