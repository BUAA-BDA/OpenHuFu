# Hu-Fu: Efficient and Secure Spatial Queries over Data Federation

## Features

Hu-Fu is the first system for efficient and secure processing of federated spatial queries. The system can parse the federated spatial query written in SQL, decompose the query into (plaintext/secure) operators and collect the query result securely. We also provide [**a demo on the OSM dataset**](hufu-demo-osm). In particular, the features of Hu-Fu are summarized as follows. (For more details, please refer to [**Hu-Fu Technical Report.pdf**](Hu-Fu_Technical_Report.pdf))

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

Here we take PostgreSQL(SQL) as an example. For the installation of other systems, please refer to [MySQL](https://dev.mysql.com/doc/mysql-installation-excerpt/5.7/en/), [SpatiaLite](https://www.gaia-gis.it/fossil/libspatialite/index), [Simba](http://www.cs.utah.edu/~dongx/simba/), [GeoMesa](https://www.geomesa.org/), [SpatialHadoop](http://spatialhadoop.cs.umn.edu/)

* Install PostgreSQL (PostGIS)

  ```bash
  sudo apt-get install postgresql-10, postgis
  ```

* Download and install [Java 8](http://openjdk.java.net/projects/jdk8/), [Maven](http://maven.apache.org/install.html) and [Python](https://www.python.org/)

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

### Running Hu-Fu Demo on OSM

In the example below, we will show how to execute federated spatial queries over a four-silo data federation with PostgreSQL.

* Create user, database and PostGIS extension in PostgreSQL

  ```sqlite
  sudo su postgres
  psql
  postgres=# CREATE DATABASE osm_db;
  postgres=# CREATE USER hufu WITH PASSWORD 'hufu';
  postgres=# GRANT ALL ON DATABASE osm_db TO hufu;
  postgres=# \c osm_db
  osm_db=# CREATE EXTENSION postgis;
  ```

* Import the data which is sampled from [OSM](https://www.openstreetmap.org/) dataset

  ```bash
  cd hufu-demo-osm/data-importer/postgresql
  python importer.py  # If a package is missing, install it using 'pip'.
  ```

* Start up drivers(Make sure the socket port used by ``hufu-demo-osm/driver/config[x].json`` is available)

  ```bash
  cd hufu-demo-osm/driver
  ./start_driver.sh 1 2 3 4  # make sure you have compiled and packaged the source code with package.sh
  ```

* Start up command line interface(CLI)

  ```bash
  cd hufu-demo-osm/cli
  ./start_cli.sh  # make sure you have compiled and packaged the source code with package.sh
  ```

  * Federated Range Query

    ```sql
    Hu-Fu> SELECT id FROM osm_a WHERE DWithin(Point(121.5, 14.5), location, 0.5);
    ```

  * Federated Range Counting

    ```sql
    Hu-Fu> SELECT COUNT(*) cnt FROM osm_a WHERE DWithin(Point(121.5, 14.5), location, 0.5);
    ```

  * Federated kNN

    ```sql
    Hu-Fu> SELECT id FROM osm_a WHERE KNN(Point(121.5, 14.5), location, 8);
    ```

  * Federated Distance Join

    ```sql
    Hu-Fu> SELECT R.id, S.id FROM osm_b R JOIN osm_a S ON DWithin(S.location, R.location, 0.2);
    ```

  * Federated kNN Join

    ```sql
    Hu-Fu> SELECT R.id, S.id FROM osm_b R JOIN osm_a S ON KNN(S.location, R.location, 8);
    ```

  * Exit

    ```sql
    Hu-Fu> !q
    ```

* Sample output of federated spatial query

  Due to limited space, we only show the output of federated Range Counting and federated kNN query.

  * Federated Range Counting
  <img src="hufu-docs/images/sample_output_rangecounting.svg" alt="sample_output" style="zoom: 50%;" />

  * Federated kNN query
  <img src="hufu-docs/images/sample_output_knn.svg" alt="sample_output" style="zoom: 50%;" />

* Stop drivers

  ```bash
  cd hufu-osm-demo/driver
  ./stop_driver.sh
  ```

### Executing  queries on your own spatial data

If you want to execute queries on your own spatial data, you should modify some configuration files as follows.

* `hufu-demo-osm/data-importer/postgresql/schema.json`: You can modify this file to import other data. You can also create tables and import data by yourself.

* `hufu-demo-osm/driver/config[x].json`: You can modify the **secure level** of each table in drivers. For details of ``config[x].json``, see [Configuration of Driver](hufu-docs/DriverConfig.md).

* `hufu-demo-osm/client/model.json`: You can change **the number of silos** in the data federation by modifying this file, see [Configuration of CLI](hufu-docs/CLIConfig.md).

### Running Hu-Fu on different physical machines

It is very easy to deploy Hu-Fu on different physical machines. You only need to start up the driver on their respective machines using the script `hufu-demo-osm/driver/start_driver.sh` and modify the ip address in `hufu-demo-osm/client/model.json`. Note that the driver port should be open on each machine.

### Running Hu-Fu on heterogeneous underlying spatial database

To simplify the installation process, the underlying databases of all silos are PostgreSQL(PostGIS) in the example. Recall that Hu-Fu can support heterogeneous underlying spatial databases. We also provide the adapters for other five spatial databases. You can install these spatial databases by referring to documentations.

* [PostGIS](https://postgis.net/)
* [Simba](http://www.cs.utah.edu/~dongx/simba/)
* [GeoMesa](https://www.geomesa.org/)
* [SpatialHadoop](http://spatialhadoop.cs.umn.edu/)
* [MySQL](https://dev.mysql.com/doc/refman/8.0/en/spatial-types.html)
* [SpatiaLite](https://www.gaia-gis.it/fossil/libspatialite/home)
