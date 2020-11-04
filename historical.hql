CREATE EXTERNAL TABLE IF NOT EXISTS card_transactions_stg (
  card_id STRING,
  member_id STRING,
  amount DOUBLE,
  postcode STRING,
  pos_id STRING,
  transaction_dt STRING,
  status STRING
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
LOCATION '/user/hadoop/historical/card_transactions'
tblproperties("skip.header.line.count"="1");

CREATE EXTERNAL TABLE IF NOT EXISTS look_up_table (
  card_id STRING,
  card_purchase_dt STRING,
  transaction_dt STRING,
  member_id STRING,
  member_joining_dt STRING,
  country STRING,
  city STRING,
  UCL DOUBLE,
  postcode STRING,
  score INT
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
STORED BY "org.apache.hadoop.hive.hbase.HBaseStorageHandler"
WITH SERDEPROPERTIES ("hbase.columns.mapping" = "card_details:card_purchase_dt, card_details:transaction_dt,Member_details:member_id, Member_details:member_joining_dt, Location:country,Location:city,Rule_params:UCL,Rule_params:postcode, Rule_params:score")
TBLPROPERTIES ("hbase.table.name" = "look_up_table");

LOAD DATA INPATH 'hdfs:/user/hadoop/hive/card_transactions.csv' OVERWRITE INTO TABLE card_transactions_stg;

CREATE EXTERNAL TABLE IF NOT EXISTS card_transactions (row_key struct<card_id:string, pos_id:string, transaction_dt:string>, card_id STRING, pos_id STRING, transaction_dt STRING, member_id STRING,amount DOUBLE, postcode STRING,status STRING)
ROW FORMAT DELIMITED
COLLECTION ITEMS TERMINATED BY '~'
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ('hbase.columns.mapping'='TD:card_id, TD:pos_id, TD:transaction_dt,TD:member_id,TD:amount, TD:postcode, TD:status')
TBLPROPERTIES ("hbase.table.name" = "card_transactions");

Insert into card_transactions  select NAMED_STRUCT('card_id',card_id,'pos_id',pos_id,'transaction_dt', transaction_dt) as row_key,card_id,pos_id,transaction_dt, member_id, amount, postcode, status from card_transactions_stg;

CREATE VIEW IF NOT EXISTS last_ten_transactions 
AS select card_id, member_id, amount, transaction_dt, postcode, rank() over (PARTITION BY card_id ORDER BY unix_timestamp(transaction_dt , 'dd-MM-yyyy hh:mm:ss') desc, amount desc) as ranking from card_transactions where status='GENUINE';