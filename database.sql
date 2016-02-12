
-- create a table in hive
CREATE EXTERNAL TABLE word (
    word STRING,
    count INT)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION 's3://mizhangproject41/output/';
-- create a table in hive



-- load data to hive
INSERT OVERWRITE LOCAL DIRECTORY '/home/hadoop/wordcount/ngrams' ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t' select * from word order by count desc limit 100;


hadoop distcp /output/* s3://mizhangproject41/output2