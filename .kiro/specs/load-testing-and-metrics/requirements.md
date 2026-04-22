# Requirements Document

## Introduction

This feature adds CloudWatch Metrics instrumentation to the Hive Glue Catalog Sync Agent and a load testing framework to validate sync correctness and performance under high-volume HMS event workloads. The metrics instrumentation provides operational visibility into queue depth, sync latency, throughput, and error rates. The load testing framework generates parameterized HMS event workloads via Spark SQL on EMR and validates sync completeness and performance using the published CloudWatch Metrics.

## Glossary

- **Sync_Agent**: The HiveGlueCatalogSyncAgent, a Hive MetaStoreEventListener that enqueues CatalogOperation objects and syncs them to the Glue Data Catalog via a background queue processor thread.
- **Queue_Processor**: The GlueCatalogQueueProcessor inner class that drains the ConcurrentLinkedQueue in batch windows and executes operations against the Glue API.
- **Metrics_Publisher**: A new component responsible for collecting, batching, and publishing metric data points to Amazon CloudWatch Metrics using the AWS SDK v1 CloudWatch client.
- **CW_Metrics**: Amazon CloudWatch Metrics, the AWS service for publishing and querying structured numeric metric data.
- **GDC**: AWS Glue Data Catalog, the target catalog that the Sync_Agent writes to.
- **CWL**: CloudWatch Logs, the existing log reporting destination used by CloudWatchLogsReporter.
- **Batch_Window**: The configurable time interval (glue.catalog.batch.window.seconds) between queue drain cycles in the Queue_Processor.
- **Load_Test_Framework**: A set of scripts that generate high-volume HMS events via Spark SQL on EMR, wait for sync completion, and validate correctness and performance.
- **Scenario**: A parameterized load test configuration specifying the number of databases, tables per database, partitions per table, and the types of DDL operations to execute.
- **Sync_Lag**: The elapsed time between when a CatalogOperation is enqueued and when the corresponding Glue API call completes.
- **Throttle**: An HTTP 429 response from the Glue API indicating rate limiting.

## Requirements

### Requirement 1: Metrics Collection Configuration

**User Story:** As an operator, I want to opt in to CloudWatch Metrics publishing via Hadoop configuration properties, so that I can monitor the sync agent without incurring unexpected CloudWatch costs.

#### Acceptance Criteria

1. WHEN the Hadoop configuration property `glue.catalog.metrics.enabled` is set to `true`, THE Sync_Agent SHALL create a Metrics_Publisher and begin collecting metric data points.
2. WHEN the Hadoop configuration property `glue.catalog.metrics.enabled` is absent or set to `false`, THE Sync_Agent SHALL not create a CloudWatch client, not collect timestamps for Sync_Lag, and impose zero additional overhead on queue processing.
3. THE Metrics_Publisher SHALL read the CloudWatch namespace from the Hadoop configuration property `glue.catalog.metrics.namespace` with a default value of `HiveGlueCatalogSync`.
4. THE Metrics_Publisher SHALL read the publish interval from the Hadoop configuration property `glue.catalog.metrics.publish.interval.seconds` with a default value of `60`.
5. THE Metrics_Publisher SHALL read the AWS region from the same `glue.catalog.region` property used by GlueClientFactory, defaulting to `us-east-1`.

### Requirement 2: Metric Data Points

**User Story:** As an operator, I want structured metrics for queue depth, batch size, sync latency, operation outcomes, retries, batch processing time, and throttle counts, so that I can build CloudWatch dashboards and alarms for operational health.

#### Acceptance Criteria

1. WHEN the Queue_Processor begins a batch drain cycle, THE Metrics_Publisher SHALL record a `QueueDepth` metric with the number of operations in the ConcurrentLinkedQueue before draining.
2. WHEN the Queue_Processor drains the queue, THE Metrics_Publisher SHALL record a `BatchSize` metric with the number of operations in the drained batch.
3. WHEN a CatalogOperation completes execution, THE Metrics_Publisher SHALL record a `SyncLagMs` metric with the elapsed time in milliseconds between the operation's enqueue timestamp and execution completion.
4. WHEN a Glue API call succeeds, THE Metrics_Publisher SHALL record an `OperationSuccess` metric with a count of 1 and a dimension `OperationType` set to the operation type name (CREATE_TABLE, DROP_TABLE, ADD_PARTITIONS, DROP_PARTITIONS, UPDATE_TABLE, CREATE_DATABASE).
5. WHEN a Glue API call fails permanently (after all retries are exhausted), THE Metrics_Publisher SHALL record an `OperationFailure` metric with a count of 1 and a dimension `OperationType` set to the operation type name.
6. WHEN a Glue API call requires retries, THE Metrics_Publisher SHALL record a `RetryCount` metric with the total number of retry attempts for that operation.
7. WHEN the Queue_Processor finishes processing a batch, THE Metrics_Publisher SHALL record a `BatchProcessingTimeMs` metric with the elapsed wall-clock time in milliseconds for the entire batch.
8. WHEN the Glue API returns an HTTP 429 response, THE Metrics_Publisher SHALL record a `ThrottleCount` metric with a count of 1.

### Requirement 3: Metrics Publishing

**User Story:** As an operator, I want metrics published to CloudWatch in batches at a configurable interval, so that API call volume is minimized and costs are predictable.

#### Acceptance Criteria

1. THE Metrics_Publisher SHALL buffer metric data points in memory and publish them to CloudWatch in a single `PutMetricData` API call per flush cycle.
2. THE Metrics_Publisher SHALL flush buffered metrics at the interval specified by `glue.catalog.metrics.publish.interval.seconds`.
3. WHEN the number of buffered metric data points exceeds the CloudWatch `PutMetricData` limit of 1000 data points per request, THE Metrics_Publisher SHALL split the data into multiple API calls.
4. WHEN the Sync_Agent shuts down, THE Metrics_Publisher SHALL flush all remaining buffered metrics before terminating.
5. IF a `PutMetricData` API call fails, THEN THE Metrics_Publisher SHALL log the error and discard the failed batch to avoid unbounded memory growth.
6. THE Metrics_Publisher SHALL publish metrics to the namespace configured via `glue.catalog.metrics.namespace`.
7. THE Metrics_Publisher SHALL use the AWS SDK v1 `AmazonCloudWatch` client created with the same region as the Glue client.

### Requirement 4: Metrics Zero-Overhead When Disabled

**User Story:** As an operator, I want the metrics feature to have zero runtime overhead when disabled, so that existing deployments are unaffected.

#### Acceptance Criteria

1. WHEN metrics are disabled, THE Sync_Agent SHALL not instantiate an `AmazonCloudWatch` client.
2. WHEN metrics are disabled, THE Sync_Agent SHALL not record enqueue timestamps on CatalogOperation objects for Sync_Lag calculation.
3. WHEN metrics are disabled, THE Sync_Agent SHALL not allocate metric buffer data structures.
4. WHEN metrics are disabled, THE Queue_Processor SHALL execute the same code path as before this feature was added, with no additional method calls or conditional checks in the hot path.

### Requirement 5: Load Test DDL Generation

**User Story:** As a developer, I want to generate parameterized Spark SQL DDL scripts that create high-volume HMS events, so that I can test the sync agent under realistic workloads.

#### Acceptance Criteria

1. WHEN given a Scenario configuration specifying database count, tables per database, and partitions per table, THE Load_Test_Framework SHALL generate Spark SQL DDL files that create the specified number of databases, tables, and partitions.
2. THE Load_Test_Framework SHALL support the following Scenario types: burst create (tables only), partition heavy (few tables with many partitions), multi-database (multiple databases with tables and partitions), mixed operations (create, alter, drop interleaving), and sustained (high table count with partitions).
3. WHEN generating mixed operations Scenarios, THE Load_Test_Framework SHALL produce DDL that creates tables, alters them, and drops a subset, exercising create-alter-drop interleaving.
4. THE Load_Test_Framework SHALL parameterize the S3 data location using a configurable S3 bucket variable.

### Requirement 6: Load Test Orchestration

**User Story:** As a developer, I want an automated orchestration script that generates DDL, submits it to EMR, waits for sync completion, and validates results, so that load tests are repeatable and require no manual intervention.

#### Acceptance Criteria

1. THE Load_Test_Framework SHALL reuse the existing EMR integration test infrastructure (CloudFormation template, bootstrap scripts, Spark SQL submission).
2. WHEN a load test is initiated, THE Load_Test_Framework SHALL generate DDL for the selected Scenario, upload artifacts to S3, and submit EMR steps.
3. WHEN EMR steps complete, THE Load_Test_Framework SHALL wait for sync completion by polling the GDC until all expected tables and partitions appear or a configurable timeout expires.
4. THE Load_Test_Framework SHALL support configuring the Batch_Window value (5s, 10s, 30s, 60s) for each test run.
5. THE Load_Test_Framework SHALL clean up all created resources (Glue databases, tables, S3 data, CloudFormation stack) on exit, including on failure.
6. THE Load_Test_Framework SHALL be runnable via a Maven profile (`-Pload-test`) consistent with the existing integration test profile pattern.

### Requirement 7: Load Test Validation

**User Story:** As a developer, I want the load test to validate sync completeness and performance against configurable thresholds, so that I can detect regressions in correctness and throughput.

#### Acceptance Criteria

1. WHEN validation runs, THE Load_Test_Framework SHALL verify 100% sync completeness by checking that every HMS table and partition created by the DDL exists in the GDC.
2. WHEN validation runs, THE Load_Test_Framework SHALL query CWL for the HIVE_METADATA_SYNC log group and verify that no entries contain DISALLOWED or permanent ERROR status.
3. WHEN CW_Metrics are enabled for the test run, THE Load_Test_Framework SHALL query CloudWatch Metrics to collect Sync_Lag, throughput (OperationSuccess count), and error rate (OperationFailure count) statistics.
4. WHEN Sync_Lag exceeds a configurable threshold (default: 300000 milliseconds for 1000 operations), THE Load_Test_Framework SHALL report the test as failed.
5. THE Load_Test_Framework SHALL produce a summary report containing: Scenario name, total operations, sync completeness percentage, average and p99 Sync_Lag, total OperationSuccess count, total OperationFailure count, total ThrottleCount, and pass/fail verdict.

### Requirement 8: Load Test Scenarios

**User Story:** As a developer, I want predefined load test scenarios that exercise different aspects of the sync agent, so that I can validate burst throughput, partition batching, cross-database parallelism, operation interleaving, and sustained load.

#### Acceptance Criteria

1. THE Load_Test_Framework SHALL include a "burst create" Scenario with 1 database, 100 tables, 0 partitions (100 total operations) to test CreateTable throughput and batch merging.
2. THE Load_Test_Framework SHALL include a "partition heavy" Scenario with 1 database, 5 tables, 200 partitions per table (1,005 total operations) to test BatchCreatePartition batching and Glue API limits.
3. THE Load_Test_Framework SHALL include a "multi-db" Scenario with 10 databases, 10 tables per database, 10 partitions per table (1,100 total operations) to test CreateDatabase and cross-database parallelism.
4. THE Load_Test_Framework SHALL include a "mixed ops" Scenario with 1 database, 50 tables, 20 partitions per table, including alter and drop operations (~150 total operations) to test create-alter-drop interleaving.
5. THE Load_Test_Framework SHALL include a "sustained" Scenario with 1 database, 500 tables, 5 partitions per table (3,000 total operations) to test queue depth, memory usage, and long-running stability.

### Requirement 9: IAM Permissions

**User Story:** As a developer, I want the EMR integration test CloudFormation template updated with the required IAM permissions, so that the sync agent can publish CloudWatch Metrics during load tests.

#### Acceptance Criteria

1. WHEN the load test CloudFormation template is deployed, THE EmrEc2Role SHALL include the `cloudwatch:PutMetricData` IAM permission.
