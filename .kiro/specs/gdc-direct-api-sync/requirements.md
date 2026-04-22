# Requirements Document

## Introduction

Refactor the Hive Glue Catalog Sync Agent to replace Athena JDBC-based DDL execution with direct AWS Glue Data Catalog (GDC) API calls. The agent listens for Hive Metastore events and synchronizes table and partition metadata to the AWS Glue Data Catalog. Currently, it converts events into Athena DDL SQL strings and executes them via JDBC. This refactoring replaces that approach with structured catalog operation objects processed by a queue consumer that calls GDC APIs directly (createTable, deleteTable, batchCreatePartition, batchDeletePartition, updateTable, createDatabase).

## Glossary

- **Sync_Agent**: The HiveGlueCatalogSyncAgent class, a Hive MetaStoreEventListener that intercepts metastore events and queues catalog operations for synchronization to the AWS Glue Data Catalog.
- **GDC_Client**: The AWS SDK AWSGlue client used to make direct API calls to the AWS Glue Data Catalog.
- **Catalog_Operation**: A structured object representing a single GDC API action (e.g., CreateTable, DropTable, AddPartition, DropPartition, UpdateTable, CreateDatabase) queued for asynchronous processing.
- **Queue_Processor**: The GlueCatalogQueueProcessor background thread that drains the ConcurrentLinkedQueue of Catalog_Operation objects and executes corresponding GDC API calls.
- **Table_Input_Builder**: A utility component that converts Hive Table metadata into AWS Glue TableInput objects, including StorageDescriptor, Column, and SerDeInfo mappings.
- **CloudWatch_Reporter**: The CloudWatchLogsReporter class that reports sync activity to CloudWatch Logs.
- **Operation_Queue**: The ConcurrentLinkedQueue that holds Catalog_Operation objects for asynchronous processing by the Queue_Processor.
- **Table_Ownership**: A table-level property (`table.system.ownership`) with values `hms` or `gdc` that indicates which system owns the table. Only tables owned by `hms` are synchronized to GDC.
- **Blacklist**: An in-memory set of fully qualified table names (database.table) that have been excluded from further sync operations due to unresolvable conflicts.

## Requirements

### Requirement 1: Catalog Operation Model

**User Story:** As a developer, I want Hive metastore events to be represented as structured catalog operation objects instead of DDL strings, so that the queue processor can call GDC APIs directly.

#### Acceptance Criteria

1. THE Sync_Agent SHALL represent each metastore event as a Catalog_Operation object containing the operation type, database name, table name, and operation-specific payload.
2. THE Catalog_Operation SHALL support the following operation types: CREATE_TABLE, DROP_TABLE, ADD_PARTITIONS, DROP_PARTITIONS, UPDATE_TABLE, and CREATE_DATABASE.
3. WHEN a Catalog_Operation is created, THE Sync_Agent SHALL populate the operation-specific payload with the data required to make the corresponding GDC API call.

### Requirement 2: Hive-to-Glue Metadata Conversion

**User Story:** As a developer, I want Hive Table metadata to be converted into AWS Glue TableInput objects, so that the GDC APIs receive correctly structured requests.

#### Acceptance Criteria

1. WHEN a Hive Table is converted, THE Table_Input_Builder SHALL map Hive column definitions (name, type, comment) to Glue Column objects.
2. WHEN a Hive Table is converted, THE Table_Input_Builder SHALL map the Hive StorageDescriptor (input format, output format, SerDe library, SerDe parameters, location) to a Glue StorageDescriptor.
3. WHEN a Hive Table is converted, THE Table_Input_Builder SHALL map Hive partition keys to Glue Column objects in the partitionKeys field of the TableInput.
4. WHEN a Hive Table is converted, THE Table_Input_Builder SHALL translate S3 location paths (s3a://, s3n://) to standard s3:// paths.
5. WHEN a Hive Table is converted, THE Table_Input_Builder SHALL preserve table parameters as Glue TableInput parameters.
6. THE Table_Input_Builder SHALL produce a TableInput that, when used with the GDC createTable API, results in a table whose columns, partition keys, storage descriptor, and parameters match the original Hive Table metadata.

### Requirement 3: Table Ownership Filter

**User Story:** As a developer, I want the sync agent to respect table ownership, so that tables owned by GDC are not overwritten by HMS events and circular updates are prevented.

#### Acceptance Criteria

1. WHEN a metastore event occurs, THE Sync_Agent SHALL read the `table.system.ownership` property from the Hive Table parameters.
2. WHEN the `table.system.ownership` property is set to `gdc`, THE Sync_Agent SHALL ignore the event and not enqueue any Catalog_Operation.
3. WHEN the `table.system.ownership` property is set to `hms` or is absent, THE Sync_Agent SHALL proceed with normal event processing.

### Requirement 4: CreateTable Event Handling

**User Story:** As a developer, I want CreateTable events to be synchronized to the Glue Data Catalog via the createTable API, so that new external tables appear in the catalog.

#### Acceptance Criteria

1. WHEN a CreateTable event occurs for a table with an S3-based location (after s3a/s3n translation), THE Sync_Agent SHALL enqueue a CREATE_TABLE Catalog_Operation containing the Glue TableInput.
2. WHEN a CreateTable event occurs for a table that does not have an S3-based location, THE Sync_Agent SHALL ignore the event.
3. WHEN the Queue_Processor processes a CREATE_TABLE operation, THE GDC_Client SHALL call the createTable API with the database name and TableInput.
4. IF the createTable API returns an AlreadyExistsException and dropTableIfExists is enabled, THEN THE Queue_Processor SHALL delete the existing table and retry the createTable call.
5. IF the createTable API returns an AlreadyExistsException and dropTableIfExists is disabled, THEN THE Queue_Processor SHALL add the table to the Blacklist, log the conflict to the CloudWatch_Reporter, and skip the operation.
6. IF the createTable API returns an EntityNotFoundException for the database and createMissingDB is enabled, THEN THE Queue_Processor SHALL create the database and retry the createTable call.

### Requirement 5: DropTable Event Handling

**User Story:** As a developer, I want DropTable events to be synchronized to the Glue Data Catalog via the deleteTable API, so that removed tables are cleaned up from the catalog.

#### Acceptance Criteria

1. WHEN a DropTable event occurs for a table with an S3-based location and suppressAllDropEvents is disabled, THE Sync_Agent SHALL enqueue a DROP_TABLE Catalog_Operation.
2. WHEN suppressAllDropEvents is enabled, THE Sync_Agent SHALL ignore all DropTable events.
3. WHEN the Queue_Processor processes a DROP_TABLE operation, THE GDC_Client SHALL call the deleteTable API with the database name and table name.
4. IF the deleteTable API returns an EntityNotFoundException, THEN THE Queue_Processor SHALL log the inconsistency to the CloudWatch_Reporter and skip the operation without error.

### Requirement 6: AddPartition Event Handling

**User Story:** As a developer, I want AddPartition events to be synchronized to the Glue Data Catalog via the batchCreatePartition API, so that new partitions are registered in the catalog.

#### Acceptance Criteria

1. WHEN an AddPartition event occurs for a table with an S3-based location, THE Sync_Agent SHALL enqueue an ADD_PARTITIONS Catalog_Operation containing the list of PartitionInput objects.
2. WHEN building PartitionInput objects, THE Sync_Agent SHALL include partition values, the S3 location (translated to s3://), and the StorageDescriptor from the partition.
3. WHEN the Queue_Processor processes an ADD_PARTITIONS operation, THE GDC_Client SHALL call the batchCreatePartition API with the database name, table name, and list of PartitionInput objects.
4. WHEN a partition in the AddPartition event does not have an S3-based location, THE Sync_Agent SHALL skip that partition.
5. IF the batchCreatePartition API returns an EntityNotFoundException for the table, THEN THE Queue_Processor SHALL log the inconsistency to the CloudWatch_Reporter and skip the operation.

### Requirement 7: DropPartition Event Handling

**User Story:** As a developer, I want DropPartition events to be synchronized to the Glue Data Catalog via the batchDeletePartition API, so that removed partitions are cleaned up from the catalog.

#### Acceptance Criteria

1. WHEN a DropPartition event occurs for a table with an S3-based location and suppressAllDropEvents is disabled, THE Sync_Agent SHALL enqueue a DROP_PARTITIONS Catalog_Operation containing the list of partition value lists to delete.
2. WHEN suppressAllDropEvents is enabled, THE Sync_Agent SHALL ignore all DropPartition events.
3. WHEN the Queue_Processor processes a DROP_PARTITIONS operation, THE GDC_Client SHALL call the batchDeletePartition API with the database name, table name, and list of PartitionValueList objects.
4. WHEN a partition in the DropPartition event does not have an S3-based location, THE Sync_Agent SHALL skip that partition.
5. IF the batchDeletePartition API returns an EntityNotFoundException for the table, THEN THE Queue_Processor SHALL log the inconsistency to the CloudWatch_Reporter and skip the operation.

### Requirement 8: AlterTable (UpdateTable) Event Handling

**User Story:** As a developer, I want AlterTable events to be synchronized to the Glue Data Catalog via the updateTable API, so that schema changes are reflected in the catalog.

#### Acceptance Criteria

1. WHEN an AlterTable event occurs for a table with an S3-based location, THE Sync_Agent SHALL enqueue an UPDATE_TABLE Catalog_Operation containing the new Glue TableInput.
2. WHEN an AlterTable event occurs for a table that does not have an S3-based location, THE Sync_Agent SHALL ignore the event.
3. WHEN the Queue_Processor processes an UPDATE_TABLE operation, THE GDC_Client SHALL call the updateTable API with the database name and the new TableInput.

### Requirement 9: Table Blacklisting

**User Story:** As a developer, I want tables that encounter unresolvable conflicts to be disallowed from further sync operations, so that repeated failures do not cause noise or wasted API calls.

#### Acceptance Criteria

1. WHEN a table is added to the Blacklist, THE Sync_Agent SHALL skip all subsequent Catalog_Operations for that table.
2. WHEN a metastore event occurs for a disallowed table, THE Sync_Agent SHALL log a debug message and not enqueue any Catalog_Operation.
3. THE Blacklist SHALL be maintained in memory for the lifetime of the Sync_Agent process.

### Requirement 10: Operation Batching

**User Story:** As a developer, I want the queue processor to batch compatible operations together before executing them, so that API load on the Glue Data Catalog is reduced.

#### Acceptance Criteria

1. THE Queue_Processor SHALL wait for a configurable batch window duration (default 60 seconds) before draining the Operation_Queue.
2. WHEN the batch window expires, THE Queue_Processor SHALL drain all available Catalog_Operations from the Operation_Queue and group them by operation type, database name, and table name.
3. WHEN multiple consecutive ADD_PARTITIONS operations exist for the same database and table, THE Queue_Processor SHALL merge them into a single batchCreatePartition API call.
4. WHEN multiple consecutive DROP_PARTITIONS operations exist for the same database and table, THE Queue_Processor SHALL merge them into a single batchDeletePartition API call.
5. WHEN multiple consecutive DROP_TABLE operations exist for the same database and table, THE Queue_Processor SHALL deduplicate them into a single deleteTable API call.
6. WHEN multiple consecutive CREATE_TABLE operations exist for the same database and table, THE Queue_Processor SHALL use only the last CREATE_TABLE operation (most recent schema).
7. WHEN multiple consecutive UPDATE_TABLE operations exist for the same database and table, THE Queue_Processor SHALL use only the last UPDATE_TABLE operation (most recent schema).
8. THE Sync_Agent SHALL read the batch window duration from the Hadoop Configuration property `glue.catalog.batch.window.seconds` with a default value of 60.

### Requirement 11: Queue Processing and Error Handling

**User Story:** As a developer, I want the queue processor to reliably execute GDC API calls with appropriate retry and error handling, so that transient failures do not cause data loss.

#### Acceptance Criteria

1. THE Queue_Processor SHALL process batched operations preserving FIFO order for operations targeting the same database and table. CREATE_DATABASE operations SHALL be executed before any operations referencing that database. Operations targeting different tables MAY be executed in any order.
2. IF a GDC API call fails with a transient error (throttling or service unavailable), THEN THE Queue_Processor SHALL retry the operation using exponential backoff with configurable initial duration, multiplier, and maximum attempts.
3. IF a GDC API call fails with a non-transient error, THEN THE Queue_Processor SHALL log the error to the CloudWatch_Reporter and move to the next operation.
4. THE Queue_Processor SHALL run as a daemon thread and shut down gracefully via a shutdown hook.

### Requirement 12: Configuration

**User Story:** As an operator, I want to configure the sync agent via Hadoop Configuration properties, so that I can control GDC API behavior without code changes.

#### Acceptance Criteria

1. THE Sync_Agent SHALL read the AWS region from the Hadoop Configuration property `glue.catalog.region` and use it to configure the GDC_Client.
2. THE Sync_Agent SHALL read an optional Glue Catalog ID from the Hadoop Configuration property `glue.catalog.id` and pass it to all GDC API calls.
3. THE Sync_Agent SHALL read the `glue.catalog.dropTableIfExists` property to control whether AlreadyExistsException triggers a drop-and-recreate.
4. THE Sync_Agent SHALL read the `glue.catalog.createMissingDB` property to control whether missing databases are auto-created.
5. THE Sync_Agent SHALL read the `glue.catalog.suppressAllDropEvents` property to control whether drop events are ignored.
6. THE Sync_Agent SHALL read the `glue.catalog.batch.window.seconds` property to control the batch window duration with a default of 60 seconds.
7. THE Sync_Agent SHALL read the `glue.catalog.syncTableStatistics` property to control whether Hive table statistics are included in the Glue TableInput parameters (default false).
8. THE Sync_Agent SHALL read the `glue.catalog.retry.initialBackoffMs` property (default 1000), `glue.catalog.retry.backoffMultiplier` property (default 2.0), and `glue.catalog.retry.maxAttempts` property (default 5) to configure exponential backoff for transient error retries.
9. THE Sync_Agent SHALL remove all Athena JDBC-specific configuration properties (athena.jdbc.url, s3.staging.dir, user key/secret).

### Requirement 13: Dependency Changes

**User Story:** As a developer, I want the project to depend on the AWS SDK Glue module instead of Athena JDBC, so that the build is clean and minimal.

#### Acceptance Criteria

1. THE pom.xml SHALL include the `aws-java-sdk-glue` dependency at the same version as other AWS SDK dependencies.
2. THE pom.xml SHALL not contain any Athena JDBC driver dependencies.
3. THE Sync_Agent source code SHALL not contain any JDBC imports or JDBC-related code.

### Requirement 14: CloudWatch Logging

**User Story:** As an operator, I want sync activity to be reported to CloudWatch Logs with operation-level detail, so that I can monitor and troubleshoot synchronization.

#### Acceptance Criteria

1. WHEN the Queue_Processor successfully executes a Catalog_Operation, THE CloudWatch_Reporter SHALL log a message containing the operation type, database name, and table name.
2. WHEN the Queue_Processor encounters an error executing a Catalog_Operation, THE CloudWatch_Reporter SHALL log a message containing the operation type, database name, table name, and error details.
