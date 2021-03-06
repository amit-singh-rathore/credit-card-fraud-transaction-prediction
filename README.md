# Credit card transaction fraud detection

## Description
Fraud detection system with Spark, Kafka, Sqoop, Hbase, Hive.

## Solution atchitecture
![Solution Architecture](https://github.com/amit-singh-rathore/credit-card-fraud-transaction-prediction/blob/master/images/arch.jpg)

## Target task

1. Ingest historical data using sqoop
2. Load data into Hive
3. Create look up table
4. Create temp view
5. Load Streaming (PoS) data from Kafka
6. Process streaming data with spark streaming
7. Update rule data in Hive

