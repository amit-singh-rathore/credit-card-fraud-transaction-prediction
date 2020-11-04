#!/bin/bash

hdfs dfs -rm -r hdfs:/user/hadoop/hive
hdfs dfs -mkdir hive
hdfs dfs -copyFromLocal /home/hadoop/card_transactions.csv /user/hadoop/hive/card_transactions.csv

echo "create 'card_transactions', 'TD'" | hbase shell -n
echo "create 'look_up_table', 'card_details', 'Member_details', 'Location', 'Rule_params'" | hbase shell -n

hive -f /home/hadoop/historical.hql

