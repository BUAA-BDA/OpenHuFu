# Hu-Fu: Efficient and Secure Spatial Queries over Data Federation

## Features

Hu-Fu is the first system for a system for efficient and secure processing of federated spatial queries. The system can parse the federated spatial query written in SQL, decompose the query into (plaintext/secure) operators and collect the query result securely. We also provide a demo on OSM dataset in [hufu-demo-osm](hufu-demo-osm) .  In particular, the features of Hu-Fu are summarized as follows. (For more details, please refer to [technical report](hufu-docs/Hu-Fu (technical report).pdf))

* **Efficient and Secure Federated Spatial Queries:** Hu-Fu uses novel decomposition plans for federated spatial queries(federated kNN, kNN join, range counting, range query, distance join)(In [hufu-core](hufu-core)).
* **An Easy to Use SQL Interface:**  Hu-Fu supports query input in SQL format(In [hufu-core](hufu-core)).
* **Supporting for Multiple Silos with Heterogeneous Databases:** Hu-Fu supports data federations with >= 2 silos, and each silo can use different spatial databases,  e.g. PostgreSQL(PostGIS), MySQL, SpatiaLite, Simba, GeoMesa and SpatialHadoop(In [hufu-driver-core](hufu-driver-core)).

## Requirements

* Ubuntu 16.04+
* Apache Maven 3.6.0+
* Java 8
* PostgreSQL 10
* PostGIS 2.4
* Python 3.6+

## Installation

### Installation of Silos' Underlying Databases

Here we take PostgreSQL(SQL) as an example. The installation of other systems please refer to [MySQL](https://dev.mysql.com/doc/mysql-installation-excerpt/5.7/en/), [SpatiaLite](https://www.gaia-gis.it/fossil/libspatialite/index), [Simba](http://www.cs.utah.edu/~dongx/simba/), [GeoMesa](https://www.geomesa.org/), [SpatialHadoop](http://spatialhadoop.cs.umn.edu/)

* Install PostgreSQL (PostGIS)

  ```bash
  sudo apt-get install postgresql-10
  sudo apt-get install postgis
  ```

* Create user, database and PostGIS extension

```bash
  sudo su postgres
  psql

  postgres=# create database osm_db;
  postgres=# create user hufu with password 'hufu';
  postgres=# grant all on database osm_db to hufu;
  postgres=# \c osm_db
  osm_db=# create extension postgis;
```

* Install Java , Maven and Python

  ```bash
  sudo apt-get install openjdk-8-jdk
  sudo apt-get install maven
  sudo apt-get install python3.6
  ```

* Clone the git repository

  ```bash
  git clone https://github.com/BUAA-BDA/Hu-Fu.git
  ```

* Compile and package the source code(Check folder ./release for packaging result)

  ```bash
  cd Hu-Fu/
  ./package.sh
  ```

## Setup

### Executing example queries

In the example below, we will show how to execute federated spatial queries over a data federation with four silos.

* Import the data which is sampled from [OSM](https://www.openstreetmap.org/) dataset

  ```bash
  cd {Path of Repository}/hufu-demo-osm/data-importer/postgresql
  python importer.py  # If a package is missing, install it leveraging 'pip'.
  ```

* Replace the strings within `< >` in `hufu-demo-osm/data-importer/postgresql/schema.json` and `hufu-demo-osm/driver/config[x].json`

* Start up drivers

  ```bash
  cd {Path of Repository}/hufu-demo-osm/driver
  ./start_driver.sh 1 2 3 4
  ```

* Start up command line interface(CLI)

  ```bash
  cd {Path of Repository}/hufu-demo-osm/cli
  ./start_cli.sh
  ```

  * Federated Range Query

    ```sql
    Hu-Fu> SELECT id FROM tablea WHERE DWithin(location, Point(121.5, 14.5), 0.5);
    ```

  * Federated Range Counting

    ```sql
    Hu-Fu> SELECT COUNT(*) cnt FROM tablea WHERE DWithin(location, Point(121.5, 14.5), 0.5);
    ```

  * Federated kNN

    ```sql
    Hu-Fu> SELECT id FROM tablea WHERE KNN(location, Point(121.5, 14.5), 8);
    ```

  * Federated Distance Join

    ```sql
    Hu-Fu> SELECT R.id, S.id FROM tableb R JOIN tablea S ON DWithin(R.location, S.location, 0.2);
    ```

  * Federated kNN Join

    ```sql
    Hu-Fu> SELECT R.id, S.id FROM tableb R JOIN tablea S ON KNN(R.location, S.location, 8);
    ```
  
* Sample output of federated spatial query

  Due to limited space, we only show the output of federated Range Counting and federated kNN query.

  * Federated Range Counting
  <img src="hufu-docs/images/sample_output_rangecounting.svg" alt="sample_output" style="zoom: 50%;" />

  * Federated kNN query
  <img src="hufu-docs/images/sample_output_knn.svg" alt="sample_output" style="zoom: 50%;" />

* Stop drivers

  ```bash
  cd {Path of Repository}/hufu-osm-demo/driver
  ./stop_driver.sh
  ```

### Executing  queries on your own spatial data

If you want to execute queries on your own spatial data, you should modify some configuration files as follows.

* `hufu-demo-osm/data-importer/postgresql/schema.json`:  You can modify this file to change the **original spatial data and table schema**.

* `hufu-demo-osm/driver/configx.json`:  You can modify the **secure level** of each table in drivers.

* `hufu-demo-osm/client/model.json`:   You can change **the number of silos** in the data federation by modifying this file.

### Running Hu-Fu on different physical machines

It is very easy to deploy Hu-Fu on different physical machines. You only need to start up the driver on their respective machines using the script `hufu-demo-osm/driver/start_driver.sh` and modify the ip address in `hufu-demo-osm/client/model.json`. Note that the driver port should be open on each machine.

### Running Hu-Fu on heterogeneous underlying spatial database

To simplify the installation process, the underlying databases of all silos are PostgreSQL(PostGIS) in example. Recall that Hu-Fu can support heterogeneous underlying spatial database. We also provide the adapters for other five spatial databases. You can install these spatial databases by referring to the documentations as below.

* [PostGIS](https://postgis.net/)
* [Simba](http://www.cs.utah.edu/~dongx/simba/)
* [GeoMesa](https://www.geomesa.org/)
* [SpatialHadoop](http://spatialhadoop.cs.umn.edu/)
* [MySQL](https://dev.mysql.com/doc/refman/8.0/en/spatial-types.html)
* [SpatiaLite](https://www.gaia-gis.it/fossil/libspatialite/home)
