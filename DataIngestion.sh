#!/bin/bash

hdfs dfs -rm -r hdfs:/user/hadoop/sqoop/import/

sqoop import --connect "jdbc:mysql://<db-instance-id>.us-east-1.rds.amazonaws.com:3306/cred_financials_data?connectionCollation=latin1_swedish_ci" --table card_member --target-dir /user/hadoop/sqoop/import/cred_financials_data/card_member --username <dbuser> -m 1 --password <password>

sqoop import --connect "jdbc:mysql://<db-instance-id>.us-east-1.rds.amazonaws.com:3306/cred_financials_data?connectionCollation=latin1_swedish_ci" --table member_score --target-dir /user/hadoop/sqoop/import/cred_financials_data/member_score --username <dbuser> -m 1 --password <password>

