DROP TABLE if exists student;
CREATE TABLE student (name STRING,
                      age INT,
                      score DOUBLE,
                      dept_name STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/opt/hive/init/data.txt' OVERWRITE INTO TABLE student;

DROP TABLE if exists time;
CREATE TABLE time (id INT,
                    test_date DATE,
                    test_timestamp TIMESTAMP)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/opt/hive/init/data2.txt' OVERWRITE INTO TABLE time;
