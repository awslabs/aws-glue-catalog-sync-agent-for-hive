-- Step 3: Alter and drop operations

-- Test 1.4: Alter table — add columns
ALTER TABLE integ_test_db.parquet_partitioned ADD COLUMNS (category STRING, score INT);

-- Test 1.5: Alter table — set properties
ALTER TABLE integ_test_db.orc_table SET TBLPROPERTIES ('custom.property' = 'test_value', 'test.owner' = 'integ_test');

-- Test 1.7: Drop partitions
ALTER TABLE integ_test_db.parquet_partitioned DROP IF EXISTS PARTITION (dt='2025-01-02', region='eu-west-1');

-- Test 1.6: Drop table (create a throwaway table first)
CREATE EXTERNAL TABLE integ_test_db.to_be_dropped (
    id INT
)
STORED AS PARQUET
LOCATION '${S3_BUCKET}/integ-test/to_be_dropped/';

INSERT INTO integ_test_db.to_be_dropped VALUES (1);
