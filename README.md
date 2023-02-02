# OpenHuFu

OpenHuFu is a multi party computation framework used for data query benchmarks.
It provides flexibility for researchers to quickly implement their algorithms.
With its help, we can quickly take the experiment and get the performance of our algorithms.


## Data Generate
1. TCP-H

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


