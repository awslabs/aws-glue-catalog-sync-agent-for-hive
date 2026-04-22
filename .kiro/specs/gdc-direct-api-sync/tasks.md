# Implementation Plan: GDC Direct API Sync

## Overview

Refactor the Hive Glue Catalog Sync Agent to replace Athena JDBC with direct AWS Glue Data Catalog API calls. Implementation proceeds incrementally: dependencies first, then core data models, conversion utilities, event handlers, queue processor with batching, error handling, and finally cleanup of legacy code.

## Tasks

- [x] 1. Update project dependencies
  - [x] 1.1 Add `aws-java-sdk-glue` dependency to pom.xml at version `${aws-java-sdk-version}` with `provided` scope
    - Add `net.jqwik:jqwik:1.7.4` and `org.mockito:mockito-core:4.11.0` as test dependencies
    - Remove the `stringtemplate` dependency (no longer needed for DDL generation)
    - _Requirements: 13.1, 13.2_

- [x] 2. Implement CatalogOperation model
  - [x] 2.1 Create `CatalogOperation.java` in `com.amazonaws.services.glue.catalog`
    - Define `OperationType` enum: CREATE_TABLE, DROP_TABLE, ADD_PARTITIONS, DROP_PARTITIONS, UPDATE_TABLE, CREATE_DATABASE
    - Implement immutable fields: type, databaseName, tableName, payload, timestamp
    - Implement `getFullTableName()` helper
    - _Requirements: 1.1, 1.2, 1.3_

- [x] 3. Implement TableInputBuilder
  - [x] 3.1 Create `TableInputBuilder.java` in `com.amazonaws.services.glue.catalog`
    - Implement `buildTableInput(Table hiveTable, boolean syncStats)` — converts Hive Table to Glue TableInput
    - Implement `buildStorageDescriptor(StorageDescriptor hiveSd)` — maps Hive SD to Glue SD including inputFormat, outputFormat, serdeInfo, location, columns, bucketColumns, sortColumns, numberOfBuckets
    - Implement `buildColumns(List<FieldSchema> fields)` — maps FieldSchema list to Glue Column list
    - Implement `buildPartitionInput(Partition partition)` — converts Hive Partition to Glue PartitionInput
    - Implement `translateLocation(String location)` — normalizes s3a/s3n to s3
    - Drop skewed info with a warning log when present
    - Exclude Hive stats keys from parameters when syncStats is false
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6_
  - [x] 3.2 Write property test: Hive-to-Glue Table Conversion Preserves Metadata
    - **Property 1: Hive-to-Glue Table Conversion Preserves Metadata**
    - Generate random Hive Tables with varying columns, partition keys, storage descriptors, and parameters
    - Verify each field in the resulting TableInput matches the source Hive Table
    - **Validates: Requirements 2.1, 2.2, 2.3, 2.5, 2.6**
  - [x] 3.3 Write property test: S3 Path Translation
    - **Property 2: S3 Path Translation Normalizes All Variants**
    - Generate random S3 paths with s3://, s3a://, s3n:// prefixes
    - Verify output always starts with s3:// with remainder unchanged
    - **Validates: Requirements 2.4**

- [x] 4. Implement GlueClientFactory
  - [x] 4.1 Create `GlueClientFactory.java` in `com.amazonaws.services.glue.catalog`
    - Implement `createClient(Configuration config)` — reads `glue.catalog.region`, builds AWSGlue client with default credential provider chain
    - _Requirements: 12.1_

- [x] 5. Checkpoint
  - Ensure all tests pass, ask the user if questions arise.

- [x] 6. Implement batch grouping and merging logic
  - [x] 6.1 Create `OperationBatcher.java` in `com.amazonaws.services.glue.catalog`
    - Implement `groupByTable(List<CatalogOperation> ops)` — groups operations by (databaseName, tableName) preserving FIFO order
    - Implement `mergeConsecutive(List<CatalogOperation> ops)` — merges consecutive same-type operations: concatenate partition lists for ADD/DROP_PARTITIONS, last-write-wins for CREATE_TABLE/UPDATE_TABLE, dedup for DROP_TABLE
    - Implement `extractCreateDatabaseOps(List<CatalogOperation> ops)` — separates CREATE_DATABASE ops to execute first
    - _Requirements: 10.2, 10.3, 10.4, 10.5, 10.6, 10.7_
  - [x] 6.2 Write property test: Batch Grouping Produces Correct Groups
    - **Property 8: Batch Grouping Produces Correct Groups**
    - Generate random lists of CatalogOperations with varying db/table names
    - Verify every operation appears in exactly one group and all ops in a group share the same db/table
    - **Validates: Requirements 10.2**
  - [x] 6.3 Write property test: Batch Merging Concatenates Consecutive Partition Operations
    - **Property 9: Batch Merging Concatenates Consecutive Partition Operations**
    - Generate sequences of consecutive ADD_PARTITIONS/DROP_PARTITIONS ops for the same table
    - Verify merged result has concatenated partition lists in order
    - **Validates: Requirements 10.3, 10.4**
  - [x] 6.4 Write property test: Batch Deduplication Uses Last-Write-Wins
    - **Property 10: Batch Deduplication Uses Last-Write-Wins for Consecutive Table Operations**
    - Generate sequences of consecutive CREATE_TABLE/UPDATE_TABLE/DROP_TABLE ops
    - Verify only the last CREATE/UPDATE is kept, DROP is deduplicated to one
    - **Validates: Requirements 10.5, 10.6, 10.7**
  - [x] 6.5 Write property test: Batch Execution Preserves Per-Table FIFO Order
    - **Property 11: Batch Execution Preserves Per-Table FIFO Order**
    - Generate random batches of mixed operations for multiple tables
    - Verify per-table FIFO order is preserved and CREATE_DATABASE ops execute before dependent table ops
    - **Validates: Requirements 11.1**

- [x] 7. Checkpoint
  - Ensure all tests pass, ask the user if questions arise.

- [x] 8. Refactor HiveGlueCatalogSyncAgent event handlers
  - [x] 8.1 Add sync eligibility, ownership, and blacklist infrastructure to HiveGlueCatalogSyncAgent
    - Change queue type from `ConcurrentLinkedQueue<String>` to `ConcurrentLinkedQueue<CatalogOperation>`
    - Add `disallowedTables` field (`Set<String>` backed by ConcurrentHashMap)
    - Add `glueClient` (AWSGlue) field, `catalogId` field, `syncTableStatistics` field
    - Add `isSyncEligible(Table)` — checks S3 location (after translation) and no custom storage handler
    - Add `isTableOwnershipValid(Table)` — checks `table.system.ownership` != `gdc`
    - Add `isDisallowed(String dbName, String tableName)` — checks blacklist set
    - Add `addToQueue(CatalogOperation)` — replaces `addToAthenaQueue`
    - Read new config properties: region, catalogId, batchWindowSeconds, syncTableStatistics, retry params
    - Remove Athena JDBC fields: athenaConnection, athenaURL, info, configureAthenaConnection()
    - _Requirements: 3.1, 3.2, 3.3, 9.1, 9.2, 12.1, 12.2, 12.3, 12.4, 12.5, 12.6, 12.7, 12.8, 12.9_
  - [x] 8.2 Refactor `onCreateTable` to build and enqueue CREATE_TABLE CatalogOperation
    - Use `isSyncEligible`, `isTableOwnershipValid`, `isDisallowed` checks
    - Use `TableInputBuilder.buildTableInput()` to create the payload
    - _Requirements: 4.1, 4.2_
  - [x] 8.3 Refactor `onDropTable` to build and enqueue DROP_TABLE CatalogOperation
    - Use eligibility, ownership, blacklist, and suppressAllDropEvents checks
    - _Requirements: 5.1, 5.2_
  - [x] 8.4 Refactor `onAddPartition` to build and enqueue ADD_PARTITIONS CatalogOperation
    - Filter partitions to only include S3-based locations
    - Use `TableInputBuilder.buildPartitionInput()` for each partition
    - _Requirements: 6.1, 6.2, 6.4_
  - [x] 8.5 Refactor `onDropPartition` to build and enqueue DROP_PARTITIONS CatalogOperation
    - Filter partitions to only include S3-based locations
    - Build PartitionValueList from partition values
    - _Requirements: 7.1, 7.2, 7.4_
  - [x] 8.6 Refactor `onAlterTable` to build and enqueue UPDATE_TABLE CatalogOperation
    - Remove `alterTableRequiresDropTable` and `createAthenaAlterTableAddColumnsStatement` logic
    - Simply enqueue UPDATE_TABLE with the new table's TableInput
    - _Requirements: 8.1, 8.2, 8.3_
  - [x] 8.7 Write property test: Event Filtering Gates on S3 Location
    - **Property 3: Event Filtering Gates on S3 Location**
    - Generate random Tables with varying locations (s3, s3a, s3n, hdfs, local) and storage handlers
    - Verify queue state matches the S3 + no-storage-handler predicate
    - **Validates: Requirements 4.1, 4.2, 5.1, 6.1, 7.1, 8.1, 8.2**
  - [x] 8.8 Write property test: Table Ownership Filtering
    - **Property 4: Table Ownership Filtering Prevents Circular Updates**
    - Generate random Tables with varying `table.system.ownership` values (gdc, hms, absent)
    - Verify only non-gdc tables produce operations
    - **Validates: Requirements 3.1, 3.2, 3.3**
  - [x] 8.9 Write property test: Disallowed Tables Excluded
    - **Property 5: Disallowed Tables Are Excluded From All Operations**
    - Generate random table names, blacklist some, invoke events
    - Verify disallowed tables produce no operations
    - **Validates: Requirements 9.1, 9.2**
  - [x] 8.10 Write property test: Suppress Drop Events
    - **Property 6: Suppress Drop Events Flag Blocks All Drop Operations**
    - Generate random Tables, toggle suppressAllDropEvents, invoke drop events
    - Verify queue state matches suppress flag
    - **Validates: Requirements 5.2, 7.2**
  - [x] 8.11 Write property test: Only S3 Partitions Included
    - **Property 7: Only S3-Based Partitions Are Included in Partition Operations**
    - Generate AddPartition/DropPartition events with mixed S3/non-S3 partitions
    - Verify only S3 partitions appear in the payload
    - **Validates: Requirements 6.4, 7.4**

- [x] 9. Checkpoint
  - Ensure all tests pass, ask the user if questions arise.

- [x] 10. Implement GlueCatalogQueueProcessor
  - [x] 10.1 Create `GlueCatalogQueueProcessor` as inner class of HiveGlueCatalogSyncAgent (replacing AthenaQueueProcessor)
    - Implement batch window sleep → drain → group → merge → execute loop
    - Use `OperationBatcher` for grouping and merging
    - Implement `executeCreateTable`, `executeDropTable`, `executeAddPartitions`, `executeDropPartitions`, `executeUpdateTable`, `executeCreateDatabase` methods calling GDC APIs
    - Pass optional `catalogId` to all GDC API requests
    - Implement exponential backoff retry for transient errors (throttling, 500/503)
    - Handle AlreadyExistsException on createTable: drop+retry if dropTableIfExists, else blacklist
    - Handle EntityNotFoundException on deleteTable/batchCreatePartition/batchDeletePartition: log and skip
    - Handle EntityNotFoundException on createTable (missing DB): createDatabase+retry if createMissingDB
    - Report all operations to CloudWatchLogsReporter
    - _Requirements: 4.3, 4.4, 4.5, 5.3, 5.4, 6.3, 6.5, 7.3, 7.5, 8.3, 10.1, 10.8, 11.1, 11.2, 11.3, 11.4_
  - [x] 10.2 Wire GlueCatalogQueueProcessor into HiveGlueCatalogSyncAgent constructor
    - Replace AthenaQueueProcessor instantiation with GlueCatalogQueueProcessor
    - Update SyncAgentShutdownRoutine to work with new processor
    - Remove Athena connection setup
    - _Requirements: 11.4_
  - [x] 10.3 Write property test: CloudWatch Log Messages Contain Required Fields
    - **Property 12: CloudWatch Log Messages Contain Required Fields**
    - Generate random CatalogOperations, execute with mock GDC client
    - Capture CWL messages and verify they contain operation type, db name, table name (and error details on failure)
    - **Validates: Requirements 14.1, 14.2**
  - [x] 10.4 Write unit tests for error handling scenarios
    - Test AlreadyExistsException + dropTableIfExists=true: verify drop + retry
    - Test AlreadyExistsException + dropTableIfExists=false: verify blacklisting
    - Test EntityNotFoundException on createTable + createMissingDB=true: verify createDatabase + retry
    - Test EntityNotFoundException on deleteTable: verify graceful skip
    - Test transient error retry with exponential backoff
    - Test non-transient error: verify logged and skipped
    - _Requirements: 4.4, 4.5, 5.4, 6.5, 7.5, 11.2, 11.3_

- [x] 11. Clean up legacy code
  - [x] 11.1 Remove all Athena JDBC code and imports
    - Remove AthenaQueueProcessor inner class
    - Remove `athenaConnection`, `athenaURL`, `info` fields
    - Remove `configureAthenaConnection()` method
    - Remove `addToAthenaQueue(String)` method
    - Remove `alterTableRequiresDropTable()` and `createAthenaAlterTableAddColumnsStatement()` static methods
    - Remove JDBC imports (java.sql.*)
    - Remove Athena-specific config constants (ATHENA_JDBC_URL, GLUE_CATALOG_S3_STAGING_DIR, GLUE_CATALOG_USER_KEY, GLUE_CATALOG_USER_SECRET, DEFAULT_ATHENA_CONNECTION_URL)
    - _Requirements: 13.2, 13.3_
  - [x] 11.2 Refactor HiveUtils
    - Remove `showCreateTable()` method (replaced by TableInputBuilder)
    - Remove `propertiesToString()` and `appendSerdeParams()` methods (DDL formatting no longer needed)
    - Keep `translateLocationToS3Path()` (delegate to TableInputBuilder.translateLocation or keep as-is)
    - Keep `getColumnNames()` if still used, otherwise remove
    - _Requirements: 13.3_
  - [x] 11.3 Update existing tests
    - Remove tests for `alterTableRequiresDropTable` and `createAthenaAlterTableAddColumnsStatement`
    - Update `testPartitionSpec` if partition spec generation changes
    - Remove the dummy no-arg constructor test pattern if no longer needed
    - _Requirements: 13.3_

- [x] 12. Final checkpoint
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests validate universal correctness properties using jqwik
- Unit tests validate specific error handling scenarios and edge cases using JUnit 4 + Mockito
