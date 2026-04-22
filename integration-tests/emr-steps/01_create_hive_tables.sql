-- Step 1: Create Hive external tables in various formats
-- These run as Spark SQL on EMR with HMS (not GDC)

-- Test 1.1: External Parquet table with partitions
CREATE DATABASE IF NOT EXISTS integ_test_db;

CREATE EXTERNAL TABLE integ_test_db.parquet_partitioned (
    id INT,
    name STRING,
    amount DOUBLE
)
PARTITIONED BY (dt STRING, region STRING)
STORED AS PARQUET
LOCATION '${S3_BUCKET}/integ-test/parquet_partitioned/';

-- Insert test data
INSERT INTO integ_test_db.parquet_partitioned PARTITION (dt='2025-01-01', region='us-east-1')
VALUES (1, 'alice', 100.0), (2, 'bob', 200.0);

INSERT INTO integ_test_db.parquet_partitioned PARTITION (dt='2025-01-02', region='eu-west-1')
VALUES (3, 'charlie', 300.0);

-- Test 1.2: External ORC table
CREATE EXTERNAL TABLE integ_test_db.orc_table (
    event_id BIGINT,
    event_type STRING,
    payload STRING,
    created_at TIMESTAMP
)
STORED AS ORC
LOCATION '${S3_BUCKET}/integ-test/orc_table/';

INSERT INTO integ_test_db.orc_table
VALUES (1, 'click', '{"page":"home"}', current_timestamp()),
       (2, 'view', '{"page":"product"}', current_timestamp());

-- Test 1.3: External CSV table with custom SerDe
CREATE EXTERNAL TABLE integ_test_db.csv_table (
    col1 STRING,
    col2 STRING,
    col3 STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    'separatorChar' = ',',
    'quoteChar' = '"',
    'escapeChar' = '\\'
)
STORED AS TEXTFILE
LOCATION '${S3_BUCKET}/integ-test/csv_table/';
