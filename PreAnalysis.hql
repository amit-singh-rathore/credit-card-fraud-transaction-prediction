CREATE EXTERNAL TABLE IF NOT EXISTS card_member ( 
  card_id STRING,
  member_id STRING,
  member_joining_dt STRING,
  card_purchase_dt STRING,
  country STRING,
  city STRING
) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
LOCATION '/user/hadoop/RDS/card_member';

CREATE EXTERNAL TABLE IF NOT EXISTS member_score (
  member_id STRING,
  score INT
)
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','
LOCATION '/user/hadoop/RDS/member_score';

LOAD DATA INPATH 'hdfs:/user/hadoop/sqoop/import/cred_financials_data/card_member' OVERWRITE  INTO TABLE card_member;
LOAD DATA INPATH 'hdfs:/user/hadoop/sqoop/import/cred_financials_data/member_score' OVERWRITE INTO TABLE member_score;

Insert into look_up_table select ltt.card_id, cm.card_purchase_dt,ltt.transaction_dt, ltt.member_id, member_joining_dt, country, city, UCL,ltt.postcode, score from last_ten_transactions ltt inner join member_score ms on ltt.member_id=ms.member_id and ltt.ranking=1 inner join card_member cm on cm.member_id=ltt.member_id and ltt.ranking=1 inner join (select card_id, avg(amount)+ (3* stddev(amount)) as UCL from last_ten_transactions where ranking<=10 group by card_id) as ucl on ltt.card_id=ucl.card_id ;

exit;
