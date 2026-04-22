-- Step 2: Create Iceberg tables via Spark SQL on EMR with HMS
-- Requires spark.sql.catalog.hive_prod = org.apache.iceberg.spark.SparkCatalog
-- and spark.sql.catalog.hive_prod.type = hive

-- Test 2.1: Unpartitioned Iceberg table
CREATE TABLE hive_prod.integ_test_db.iceberg_unpartitioned (
    id INT,
    name STRING,
    value DOUBLE,
    updated_at TIMESTAMP
)
USING iceberg
LOCATION '${S3_BUCKET}/integ-test/iceberg_unpartitioned/'
TBLPROPERTIES ('format-version' = '2');

INSERT INTO hive_prod.integ_test_db.iceberg_unpartitioned
VALUES (1, 'row1', 10.0, current_timestamp()),
       (2, 'row2', 20.0, current_timestamp());

-- Test 2.2: Partitioned Iceberg table
CREATE TABLE hive_prod.integ_test_db.iceberg_partitioned (
    id INT,
    name STRING,
    amount DOUBLE,
    event_date DATE
)
USING iceberg
PARTITIONED BY (event_date)
LOCATION '${S3_BUCKET}/integ-test/iceberg_partitioned/'
TBLPROPERTIES ('format-version' = '2');

INSERT INTO hive_prod.integ_test_db.iceberg_partitioned
VALUES (1, 'a', 100.0, DATE '2025-01-01'),
       (2, 'b', 200.0, DATE '2025-01-02'),
       (3, 'c', 300.0, DATE '2025-01-01');

-- Test 2.3: Iceberg schema evolution
CREATE TABLE hive_prod.integ_test_db.iceberg_evolve (
    id INT,
    name STRING
)
USING iceberg
LOCATION '${S3_BUCKET}/integ-test/iceberg_evolve/'
TBLPROPERTIES ('format-version' = '2');

INSERT INTO hive_prod.integ_test_db.iceberg_evolve VALUES (1, 'before_evolve');

-- Schema evolution: add column
ALTER TABLE hive_prod.integ_test_db.iceberg_evolve ADD COLUMNS (score DOUBLE);

INSERT INTO hive_prod.integ_test_db.iceberg_evolve VALUES (2, 'after_evolve', 99.9);

-- Test 2.4: Iceberg snapshot operations
CREATE TABLE hive_prod.integ_test_db.iceberg_snapshots (
    id INT,
    status STRING
)
USING iceberg
LOCATION '${S3_BUCKET}/integ-test/iceberg_snapshots/'
TBLPROPERTIES ('format-version' = '2');

INSERT INTO hive_prod.integ_test_db.iceberg_snapshots VALUES (1, 'active');
INSERT INTO hive_prod.integ_test_db.iceberg_snapshots VALUES (2, 'active');
-- This creates a new snapshot
DELETE FROM hive_prod.integ_test_db.iceberg_snapshots WHERE id = 1;
