DROP TABLE if exists student;
CREATE TABLE student (name STRING,
                      age INT,
                      score INT,
                      dept_name STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/opt/hive/init/data.txt' OVERWRITE INTO TABLE student;
