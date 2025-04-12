CREATE EXTERNAL TABLE IF NOT EXISTS `carpark`.`carpark_availability` (
  `carpark_id` string,
  `area` string,
  `development` string,
  `location` string,
  `available_lots` int,
  `lot_type` string,
  `agency` string,
  `timestamp` timestamp,
  `source` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
WITH SERDEPROPERTIES ('field.delim' = ',')
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://carpark-availability-1303/'
TBLPROPERTIES (
  'classification' = 'csv',
  'skip.header.line.count' = '1'
);

select * from carpark_availability WHERE 
    "carpark_id" = '1'  order by timestamp desc;