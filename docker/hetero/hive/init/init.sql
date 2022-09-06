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

DROP TABLE if exists types;
CREATE TABLE types (a TINYINT,
                   b SMALLINT,
                   c INT,
                   d BIGINT,
                   e BOOLEAN,
                   f FLOAT,
                   g DOUBLE,
                   h DECIMAL,
                   i STRING,
                   j CHAR(4),
                   k VARCHAR(16),
                   l TIMESTAMP,
                   n DATE,
                   o BINARY,
                   p INTEGER);
