# Implementation Plan: Load Testing and Metrics

## Overview

Implement CloudWatch Metrics instrumentation for the sync agent and a load testing framework. The Java metrics components are built first (interface, NoOp, publisher, factory, integration), followed by the Python/Bash load test scripts. Property-based tests validate core metrics logic and DDL generation.

## Tasks

- [x] 1. Create MetricsCollector interface and NoOpMetricsCollector
  - [x] 1.1 Create `MetricsCollector` interface in `src/main/java/com/amazonaws/services/glue/catalog/MetricsCollector.java`
    - Define all methods: `recordQueueDepth`, `recordBatchSize`, `recordSyncLagMs`, `recordOperationSuccess`, `recordOperationFailure`, `recordRetryCount`, `recordBatchProcessingTimeMs`, `recordThrottleCount`, `flush`, `shutdown`
    - All methods must be thread-safe by contract (documented in Javadoc)
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.6, 2.7, 2.8_
  - [x] 1.2 Create `NoOpMetricsCollector` in `src/main/java/com/amazonaws/services/glue/catalog/NoOpMetricsCollector.java`
    - Implement `MetricsCollector` with empty method bodies
    - _Requirements: 4.1, 4.2, 4.3_

- [x] 2. Create MetricsPublisher
  - [x] 2.1 Add `aws-java-sdk-cloudwatch` dependency to `pom.xml`
    - Add `com.amazonaws:aws-java-sdk-cloudwatch:${aws-java-sdk-version}` with `provided` scope
    - _Requirements: 3.7_
  - [x] 2.2 Create `MetricsPublisher` in `src/main/java/com/amazonaws/services/glue/catalog/MetricsPublisher.java`
    - Implement `MetricsCollector` interface
    - Use `ConcurrentLinkedQueue<MetricDatum>` as the buffer
    - Create `AmazonCloudWatch` client from Hadoop Configuration (same region as Glue client)
    - Read namespace from `glue.catalog.metrics.namespace` (default: `HiveGlueCatalogSync`)
    - Read publish interval from `glue.catalog.metrics.publish.interval.seconds` (default: 60)
    - Each `recordXxx` method creates a `MetricDatum` with correct name, value, unit, timestamp, and dimensions (OperationType where applicable)
    - `flush()` drains buffer and calls `PutMetricData`, splitting at 1000 data points per request
    - `shutdown()` cancels scheduler, performs final flush
    - On `PutMetricData` failure: log WARN and discard batch
    - Use `ScheduledExecutorService` with a single daemon thread for periodic flush
    - _Requirements: 1.3, 1.4, 1.5, 2.1-2.8, 3.1-3.6_
  - [x] 2.3 Write property test: Factory returns correct implementation
    - **Property 1: Factory returns correct implementation based on config**
    - **Validates: Requirements 1.1, 1.2**
  - [x] 2.4 Write property test: Namespace and interval configuration
    - **Property 2: Namespace and interval configuration pass-through**
    - **Validates: Requirements 1.3, 1.4, 3.6**
  - [x] 2.5 Write property test: Flush batching
    - **Property 7: Flush batches metrics into correct number of API calls**
    - **Validates: Requirements 3.1, 3.3**
  - [x] 2.6 Write property test: Shutdown flushes all
    - **Property 8: Shutdown flushes all remaining metrics**
    - **Validates: Requirements 3.4**

- [x] 3. Create MetricsCollectorFactory
  - [x] 3.1 Create `MetricsCollectorFactory` in `src/main/java/com/amazonaws/services/glue/catalog/MetricsCollectorFactory.java`
    - `create(Configuration)` returns `MetricsPublisher` when enabled, `NoOpMetricsCollector` when disabled
    - Define config key constants: `METRICS_ENABLED`, `METRICS_NAMESPACE`, `METRICS_PUBLISH_INTERVAL`
    - _Requirements: 1.1, 1.2_

- [x] 4. Integrate MetricsCollector into GlueCatalogQueueProcessor
  - [x] 4.1 Modify `GlueCatalogQueueProcessor` constructor to accept a `MetricsCollector` parameter
    - Store as a field, call from instrumentation points
    - _Requirements: 2.1-2.8_
  - [x] 4.2 Add instrumentation calls in `GlueCatalogQueueProcessor.run()`
    - Record `QueueDepth` before drain loop, `BatchSize` after drain, `BatchProcessingTimeMs` after `processBatch()`
    - _Requirements: 2.1, 2.2, 2.7_
  - [x] 4.3 Add instrumentation calls in `executeOperation()` and retry logic
    - Record `OperationSuccess`/`OperationFailure` with OperationType dimension
    - Record `SyncLagMs` on completion
    - Record `RetryCount` after retries
    - Record `ThrottleCount` on 429 detection in `isTransientError()`
    - _Requirements: 2.3, 2.4, 2.5, 2.6, 2.8_
  - [x] 4.4 Update `HiveGlueCatalogSyncAgent` constructor to create `MetricsCollector` via factory and pass to `GlueCatalogQueueProcessor`
    - _Requirements: 1.1, 1.2_
  - [x] 4.5 Update `SyncAgentShutdownRoutine` to call `metricsCollector.shutdown()` on agent shutdown
    - _Requirements: 3.4_
  - [x] 4.6 Write property test: Queue depth and batch size recording
    - **Property 3: Queue depth and batch size recording**
    - **Validates: Requirements 2.1, 2.2**
  - [x] 4.7 Write property test: Operation outcome metrics
    - **Property 5: Operation outcome metrics record correct type dimension**
    - **Validates: Requirements 2.4, 2.5, 2.6, 2.8**

- [x] 5. Checkpoint - Ensure all Java tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 6. Create load test DDL generator
  - [x] 6.1 Create `integration-tests/load-test/generate-load.py`
    - Accept CLI args: `--scenario`, `--output-dir`, `--s3-bucket`, optional overrides for `--databases`, `--tables-per-db`, `--partitions-per-table`
    - Define SCENARIOS dict with all 5 scenarios (burst-create, partition-heavy, multi-db, mixed-ops, sustained)
    - Generate Spark SQL DDL: CREATE DATABASE, CREATE TABLE with LOCATION, ALTER TABLE ADD PARTITION
    - For mixed-ops: generate CREATE, ALTER (add column), DROP for a subset of tables
    - Parameterize S3 LOCATION with the provided bucket
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 8.1-8.5_
  - [x] 6.2 Write property test: DDL generation statement counts
    - **Property 9: DDL generation produces correct statement counts**
    - **Validates: Requirements 5.1**
  - [x] 6.3 Write property test: S3 bucket parameterization
    - **Property 10: DDL generation parameterizes S3 bucket**
    - **Validates: Requirements 5.4**

- [x] 7. Create load test validation script
  - [x] 7.1 Create `integration-tests/load-test/validate-load.py`
    - Accept CLI args: `--database`, `--region`, `--scenario`, `--expected-tables`, `--expected-partitions`, `--metrics-namespace`, `--sync-lag-threshold-ms`, `--s3-output`
    - Poll GDC for table/partition completeness
    - Query CWL for DISALLOWED/ERROR entries
    - Query CW Metrics for SyncLagMs, OperationSuccess, OperationFailure, ThrottleCount statistics
    - Compute sync completeness percentage
    - Check sync lag against threshold (default: 300000ms)
    - Produce JSON summary report with all required fields
    - Exit 0 on pass, 1 on fail
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5_
  - [x] 7.2 Write property test: Sync completeness validation
    - **Property 11: Sync completeness validation**
    - **Validates: Requirements 7.1**
  - [x] 7.3 Write property test: CWL error detection
    - **Property 12: CWL error detection**
    - **Validates: Requirements 7.2**
  - [x] 7.4 Write property test: Sync lag threshold enforcement
    - **Property 13: Sync lag threshold enforcement**
    - **Validates: Requirements 7.4**
  - [x] 7.5 Write property test: Validation report completeness
    - **Property 14: Validation report completeness**
    - **Validates: Requirements 7.5**

- [x] 8. Create load test orchestration script
  - [x] 8.1 Create `integration-tests/load-test/run-load-test.sh`
    - Accept env vars: `TEST_S3_BUCKET`, `SUBNET_ID`, `AWS_REGION`, `SCENARIO`, `BATCH_WINDOW_SECONDS`
    - Reuse existing CFN template pattern from `run-integ-tests.sh`
    - Steps: generate DDL → upload to S3 → deploy CFN stack (with metrics enabled + configurable batch window) → submit EMR steps → wait for sync → run validation → cleanup
    - Trap EXIT for cleanup (delete Glue databases/tables, delete CFN stack, clean S3)
    - Support running multiple scenarios sequentially
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5_

- [x] 9. Update CFN template and Maven build
  - [x] 9.1 Update `integration-tests/cfn/emr-integ-test.yaml` to add `cloudwatch:PutMetricData` permission to EmrEc2Role
    - Add to the GlueCatalogAccess policy
    - _Requirements: 9.1_
  - [x] 9.2 Add `load-test` Maven profile to `pom.xml`
    - Mirror the `integration-test` profile structure
    - Add `SCENARIO` and `BATCH_WINDOW_SECONDS` environment variables
    - Execute `integration-tests/load-test/run-load-test.sh`
    - _Requirements: 6.6_

- [x] 10. Create load test README
  - [x] 10.1 Create `integration-tests/load-test/README.md`
    - Document usage, scenarios, configuration, pass/fail criteria
    - Include example Maven commands for each scenario
    - _Requirements: 5.2, 8.1-8.5_

- [x] 11. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional and can be skipped for faster MVP
- Each task references specific requirements for traceability
- Checkpoints ensure incremental validation
- Property tests for Python scripts (Properties 9-14) should use pytest + hypothesis; for Java (Properties 1-8) use jqwik
- The load test framework reuses existing EMR infrastructure — no new CFN resources beyond IAM permission updates
