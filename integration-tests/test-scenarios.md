# Integration Test Scenarios

## Overview

Each test scenario performs catalog operations via Spark on EMR (writing to HMS),
waits for the sync agent to push to GDC, then validates the GDC entry via Athena.

Validation checks per scenario:
- **Metadata match**: Compare HMS table properties vs GDC table properties
- **Athena readability**: Run `SELECT * FROM <table> LIMIT 10` via Athena
- **Schema correctness**: Compare column names, types, and partition keys
- **Data correctness**: Compare row counts and sample data between Spark and Athena

---

## 1. Hive External Tables

### 1.1 Create external Parquet table with partitions
- Spark creates an external table backed by Parquet on S3
- Add multiple partitions with data
- **Validate**: Athena can read data, partition pruning works, schema matches

### 1.2 Create external ORC table
- Spark creates an external ORC table on S3
- **Validate**: Athena reads ORC data, SerDe info is correct in GDC

### 1.3 Create external CSV table with custom SerDe
- Spark creates a CSV table with OpenCSVSerde
- **Validate**: Athena reads CSV data, SerDe parameters (separatorChar, quoteChar) preserved

### 1.4 Alter table — add columns
- Start with a 3-column Parquet table
- ALTER TABLE ADD COLUMNS to add 2 more
- **Validate**: GDC schema updated, Athena sees new columns, old data returns nulls for new cols

### 1.5 Alter table — change table properties
- Set custom table properties via ALTER TABLE SET TBLPROPERTIES
- **Validate**: Properties appear in GDC table parameters

### 1.6 Drop table
- Create a table, verify it syncs, then DROP TABLE
- **Validate**: Table removed from GDC (Athena returns EntityNotFoundException)

### 1.7 Add and drop partitions
- Create partitioned table, add partitions, drop some partitions
- **Validate**: Only remaining partitions visible in GDC/Athena

---

## 2. Iceberg Tables

### 2.1 Create Iceberg table (unpartitioned)
- Spark creates an Iceberg table using HiveCatalog
- Insert data via Spark
- **Validate**:
  - GDC has `table_type=ICEBERG` in parameters
  - `metadata_location` parameter points to valid Iceberg metadata JSON on S3
  - Athena can read the table (`SELECT * FROM <table>`)
  - Schema matches between HMS and GDC

### 2.2 Create Iceberg table (partitioned)
- Spark creates a partitioned Iceberg table (e.g., partitioned by date)
- Insert data spanning multiple partitions
- **Validate**:
  - Athena can query with partition filter
  - GDC does NOT have Hive-style partition keys (Iceberg manages partitions internally)
  - `metadata_location` is valid

### 2.3 Iceberg schema evolution
- Create Iceberg table, then ALTER TABLE to add/rename/reorder columns
- **Validate**:
  - `metadata_location` updated in GDC after schema change
  - Athena reads evolved schema correctly
  - Old data still readable with new schema

### 2.4 Iceberg snapshot operations
- Insert data, then INSERT OVERWRITE or DELETE to create new snapshots
- **Validate**:
  - `metadata_location` updated in GDC
  - Athena reads latest snapshot
  - `previous_metadata_location` preserved if present

### 2.5 Iceberg table with s3a:// location
- Create Iceberg table where HMS stores location as s3a://
- **Validate**:
  - GDC location translated to s3://
  - `metadata_location` parameter also translated to s3://
  - Athena can read the table

### 2.6 Iceberg table — Athena-to-Glue readability (regression)
- This is the specific regression test for the issue found:
  Iceberg tables created in Athena are not readable by Glue w/ Lake Formation,
  but the reverse should work.
- Create Iceberg table via Spark/HMS, sync to GDC
- Read via Athena (should work)
- Read via Glue crawler / Spark with GDC catalog (should work)
- Compare table properties with an Athena-created Iceberg table to identify diffs

---

## 3. Table Ownership & Conflict Handling

### 3.1 GDC-owned table is not overwritten
- Pre-create a table in GDC with `table.system.ownership=gdc`
- Create a table with the same name in HMS
- **Validate**: GDC table is unchanged (sync agent skips it)

### 3.2 AlreadyExistsException with dropTableIfExists=true
- Pre-create a table in GDC
- Create a table with the same name in HMS (with dropTableIfExists enabled)
- **Validate**: GDC table is replaced with HMS version

### 3.3 AlreadyExistsException with dropTableIfExists=false
- Pre-create a table in GDC
- Create a table with the same name in HMS (with dropTableIfExists disabled)
- **Validate**: Table is disallowed, GDC table unchanged

---

## 4. Database Operations

### 4.1 Auto-create missing database
- Create a table in a database that doesn't exist in GDC (with createMissingDB=true)
- **Validate**: Database created in GDC, table synced successfully

### 4.2 Missing database with createMissingDB=false
- Create a table in a database that doesn't exist in GDC (with createMissingDB=false)
- **Validate**: Operation fails gracefully, logged to CloudWatch

---

## 5. Batch Operations

### 5.1 Bulk partition add (>100 partitions)
- Create a partitioned table and add 150+ partitions in rapid succession
- **Validate**: All partitions appear in GDC, batchCreatePartition handles pagination

### 5.2 Rapid schema changes
- ALTER TABLE multiple times in quick succession within a single batch window
- **Validate**: GDC reflects the final schema (last-write-wins)

---

## 6. Edge Cases

### 6.1 Non-S3 location table is skipped
- Create a table with HDFS location
- **Validate**: Table does NOT appear in GDC

### 6.2 Table with skewed columns
- Create a table with SKEWED BY clause
- **Validate**: Table syncs to GDC but skewed info is dropped (warning logged)

### 6.3 Table with bucketing
- Create a CLUSTERED BY / SORTED BY table
- **Validate**: Bucket columns and sort columns appear in GDC StorageDescriptor

---

## Validation Script Pseudocode

```python
def validate_table(db, table, athena_client, glue_client, spark_session):
    # 1. Get HMS metadata via Spark
    hms_meta = spark_session.sql(f"DESCRIBE FORMATTED {db}.{table}")

    # 2. Get GDC metadata via Glue API
    gdc_meta = glue_client.get_table(DatabaseName=db, Name=table)

    # 3. Compare schemas
    assert hms_columns == gdc_columns

    # 4. Compare table properties (with known diffs filtered)
    assert hms_params is subset of gdc_params

    # 5. Athena readability
    result = athena_client.start_query_execution(
        QueryString=f"SELECT * FROM {db}.{table} LIMIT 10")
    assert result.status == 'SUCCEEDED'

    # 6. For Iceberg: validate metadata_location
    if gdc_meta.parameters.get('table_type') == 'ICEBERG':
        assert 'metadata_location' in gdc_meta.parameters
        assert gdc_meta.parameters['metadata_location'].startswith('s3://')
```
