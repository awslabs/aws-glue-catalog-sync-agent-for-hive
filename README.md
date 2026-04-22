# Hive Glue Catalog Sync Agent

The Hive Glue Catalog Sync Agent is a software module that can be installed and configured within a Hive Metastore server, and provides outbound synchronisation to the [AWS Glue Data Catalog](https://aws.amazon.com/glue) for tables stored on Amazon S3. This enables you to seamlessly create objects on the AWS Catalog as they are created within your existing Hadoop/Hive environment without any operational overhead or tasks.

This project provides a jar that implements the [MetastoreEventListener](https://hive.apache.org/javadocs/r1.2.2/api/org/apache/hadoop/hive/metastore/MetaStoreEventListener.html) interface of Hive to capture create, alter, and drop events for databases, tables, and partitions in your Hive Metastore. It then calls the AWS Glue Data Catalog API directly to replicate those changes, providing near-real-time synchronisation. Your Hive Metastore and Yarn cluster can be anywhere — on the cloud or on your own data center.

![architecture](architecture.png)

Within the [HiveGlueCatalogSyncAgent](src/main/java/com/amazonaws/services/glue/catalog/HiveGlueCatalogSyncAgent.java), metastore events are captured as structured `CatalogOperation` objects and written to a bounded `LinkedBlockingQueue` (default capacity: 50,000, configurable). A separate queue processor thread drains the queue in configurable batch windows, groups and merges operations per table for efficiency, and executes them against the Glue Data Catalog API with exponential-backoff retry. When the queue is full, operations are rejected and logged to a dedicated machine-parseable logger for downstream manual sync.

For a more detailed architecture diagram, please see [here](docs/architecture-diagram.md)

Operational metrics (queue depth, batch size, sync lag, success/failure counts, throttle events, queue rejections) are published to Amazon CloudWatch, and sync activity is logged to CloudWatch Logs.

## Supported Events

The Catalog Sync Agent supports the following MetaStore events:

* CreateTable
* AlterTable (UPDATE_TABLE)
* DropTable
* AddPartition
* DropPartition
* CreateDatabase (auto-created when a table references a missing database, if `glue.catalog.createMissingDB` is enabled)

## Installation

Build the software with Maven:

```bash
mvn clean package assembly:assembly -DskipTests
```

This produces:
- `target/HiveGlueCatalogSyncAgent-1.3.2-SNAPSHOT.jar` — base JAR (requires dependencies on the classpath)
- `target/HiveGlueCatalogSyncAgent-1.3.2-SNAPSHOT-complete.jar` — fat JAR with all dependencies included

## Required Dependencies

If you install the base JAR (not the fat JAR), you must ensure the following dependencies are available on the classpath:

* SLF4J (1.7.36+)
* Logback (1.2.3+)
* AWS Java SDK Core, Glue, CloudWatch, CloudWatch Logs (1.12.177+)
* Hive Metastore (3.1.2)
* Hive Common (3.1.2)
* Hive Exec (3.1.2)

## Notes

### Eligable Tables To Sync
Not all tables will be synced from HMS to GDC. The criteria is available in isSyncEligible(), and isTableOwnershipValid() in [HiveGlueCatalogSyncAgent](src/main/java/com/amazonaws/services/glue/catalog/HiveGlueCatalogSyncAgent.java):
* Tables without SerDe's specified do not get synced
* Tables without StorageHandlers do not get synced
* Tables that do not use S3 do not get synced
* Tables where the table property "table.system.ownership" to "gdc" does not get synced.
* Disallowed tables (tables that exist in GDC but a Create Table is issued for the same table). 

### Recommended steps
These steps are highly recommended:
* Increase the JVM memory of your Hive Metastore by 256-512mb. This will ensure that your HMS will have sufficent memory and will not OOM from the extra records that are stored.
* Set up alarms on queue depth (QueueDepth), failure count (OperationFailure), queue rejection (QueueRejection) and throttle count (ThrottleCount) and take appropriate action. 
** For OperationFailure, and QueueRejection errors, you will need to manually sync the objects that failed to sync. 
* For any table objects that are created and managed by Glue Data Catalog, set the table property "table.system.ownership" to "gdc", and this resource does not get sync'ed once its updated in HMS. This precents circular syncing.
* Have tables created within HMS have a table property "table.system.ownership" set to "hms". This way, any syncs coming from Glue Data Catalog will be filtered to avoid circular syncing.

## Configuration Instructions

### IAM

Create an IAM policy with the following permissions:

````json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "GlueCatalogAccess",
            "Effect": "Allow",
            "Action": [
                "glue:CreateDatabase",
                "glue:DeleteDatabase",
                "glue:GetDatabase",
                "glue:GetDatabases",
                "glue:UpdateDatabase",
                "glue:CreateTable",
                "glue:DeleteTable",
                "glue:BatchDeleteTable",
                "glue:UpdateTable",
                "glue:GetTable",
                "glue:GetTables",
                "glue:BatchCreatePartition",
                "glue:CreatePartition",
                "glue:DeletePartition",
                "glue:BatchDeletePartition",
                "glue:UpdatePartition",
                "glue:GetPartition",
                "glue:GetPartitions",
                "glue:BatchGetPartition"
            ],
            "Resource": [ "arn:aws:glue:[AWS Region]:[AWS Account ID]:catalog",
                          "arn:aws:glue:[AWS Region]:[AWS Account ID]:database/*",
                          "arn:aws:glue:[AWS Region]:[AWS Account ID]:table/*/*" ]
        },
        {
            "Sid": "CloudWatchLogs",
            "Effect": "Allow",
            "Action": [
                "logs:CreateLogGroup",
                "logs:CreateLogStream",
                "logs:DescribeLogGroups",
                "logs:DescribeLogStreams",
                "logs:PutLogEvents"
            ],
            "Resource": "arn:aws:logs:[AWS Region]:[AWS Account ID]:log-group:HIVE_METADATA_SYNC:*"
        },
        {
            "Sid": "CloudWatchMetrics",
            "Effect": "Allow",
            "Action": [
                "cloudwatch:PutMetricData"
            ],
            "Resource": "*",
            "Condition": {
                "StringEquals": {
                    "cloudwatch:namespace": "HiveGlueCatalogSync"
                }
            }
        }
    ]
}
````

Then:

1. Create an IAM role and attach the policy to it
2. If your Hive Metastore runs on EC2 or EMR, attach the IAM Role to the instance profile. Otherwise, create an IAM user and generate an access and secret key.

### Hive Configuration

Add the following keys to `hive-site.xml`:

| Property | Required | Default | Description |
|---|---|---|---|
| `hive.metastore.event.listeners` | Yes | — | Set to `com.amazonaws.services.glue.catalog.HiveGlueCatalogSyncAgent` |
| `glue.catalog.id` | No | *(account default)* | Glue Data Catalog ID (for cross-account sync) |
| `glue.catalog.dropTableIfExists` | No | `false` | Drop and recreate a table if it already exists in GDC |
| `glue.catalog.createMissingDB` | No | `true` | Auto-create databases in GDC if they don't exist |
| `glue.catalog.suppressAllDropEvents` | No | `false` | Suppress all DropTable and DropPartition events |
| `glue.catalog.syncTableStatistics` | No | `false` | Include table statistics in synced table metadata |
| `glue.catalog.batch.window.seconds` | No | `60` | Seconds between queue drain cycles |
| `glue.catalog.queue.capacity` | No | `50000` | Maximum number of operations the bounded queue can hold |
| `glue.catalog.retry.maxAttempts` | No | `5` | Max retry attempts for transient Glue API errors |
| `glue.catalog.retry.initialBackoffMs` | No | `1000` | Initial backoff in ms before first retry |
| `glue.catalog.retry.backoffMultiplier` | No | `2.0` | Exponential backoff multiplier |
| `glue.catalog.metrics.enabled` | No | `false` | Enable CloudWatch Metrics publishing |
| `glue.catalog.metrics.namespace` | No | `HiveGlueCatalogSync` | CloudWatch Metrics namespace |
| `glue.catalog.metrics.publish.interval.seconds` | No | `60` | How often metrics are flushed to CloudWatch |

Add the sync agent JAR to the HMS classpath and restart. You should see newly created external tables and partitions replicated to the Glue Data Catalog, with activity logged to the `HIVE_METADATA_SYNC` CloudWatch Logs group.

### Queue backpressure and rejected operations

The operation queue is bounded (default 50,000). When the queue is full, new operations are rejected rather than blocking the Hive event thread. Rejected operations are:

- Logged to the main logger at ERROR level
- Recorded as a `QueueRejection` CloudWatch metric (dimensioned by operation type)
- Written to a dedicated logger (`com.amazonaws.services.glue.catalog.RejectedOperations`) in machine-parseable TSV format:

```
database_name\ttable_name\toperation_type\ttimestamp_ms
```

Configure your logging framework to route this logger to a separate file (e.g., `rejected-operations.log`), then feed it into a downstream sync job to reconcile any objects that fell out of the queue.

The queue processor also logs a warning when depth exceeds 80% of capacity, giving early warning before rejections begin.

## Observability

When metrics are enabled, the following CloudWatch metrics are published under the configured namespace:

| Metric | Unit | Description |
|---|---|---|
| `QueueDepth` | Count | Queue size before each drain cycle |
| `BatchSize` | Count | Number of operations drained per batch |
| `SyncLagMs` | Milliseconds | Time from enqueue to execution completion |
| `BatchProcessingTimeMs` | Milliseconds | Wall-clock time to process a batch |
| `OperationSuccess` | Count | Successful Glue API calls (dimensioned by OperationType) |
| `OperationFailure` | Count | Permanently failed Glue API calls (dimensioned by OperationType) |
| `RetryCount` | Count | Retry attempts per operation (dimensioned by OperationType) |
| `ThrottleCount` | Count | HTTP 429 responses from the Glue API |
| `QueueRejection` | Count | Operations rejected due to a full queue (dimensioned by OperationType) |

## Integration Tests

The project includes end-to-end integration tests that provision an EMR cluster with a standalone Hive Metastore, run catalog operations via Spark, sync to the Glue Data Catalog, and validate the results.

### What the tests cover

- Hive external tables (Parquet, ORC, CSV) with partitions, schema changes, and drops
- Iceberg tables (unpartitioned, partitioned, schema evolution, snapshot operations)
- Iceberg-specific validation: `table_type=ICEBERG`, `metadata_location` with `s3://` prefix, Athena readability
- Table ownership filtering, conflict handling, and batch operations
- Full list of scenarios in `integration-tests/test-scenarios.md`

### Prerequisites

- AWS CLI configured with credentials that can create EMR clusters, IAM roles, Glue catalog entries, and run Athena queries
- Python 3 with `boto3` installed (for the validation script)
- A VPC subnet ID where the EMR cluster will be launched (must have connectivity to AWS service endpoints — see `integration-tests/README.md` for details)
- An S3 bucket for test data, EMR logs, and the sync agent JAR
- If using Lake Formation: the EMR EC2 instance profile role needs Database Creator and Data Location permissions (see `integration-tests/README.md` for details)

### Running via Maven

The integration tests are behind a Maven profile and won't run during normal builds. To run them:

```bash
# Build the project first (produces the fat JAR)
mvn clean package

# Run integration tests
mvn verify -Pintegration-test \
  -Dtest.s3.bucket=my-test-bucket \
  -Dtest.subnet.id=subnet-0abc123def456
```

Optional parameters:

| Property | Default | Description |
|---|---|---|
| `test.aws.region` | `us-east-1` | AWS region for EMR and Glue |
| `test.emr.release` | `emr-7.1.0` | EMR release label |
| `test.catalog.id` | *(account default)* | Glue Catalog ID |

### What happens during the run

1. The sync agent fat JAR, bootstrap script, and SQL step scripts are uploaded to S3
2. A CloudFormation stack (`integration-tests/cfn/emr-integ-test.yaml`) creates an EMR cluster with:
   - Standalone HMS (Derby-backed, not Glue as the metastore)
   - A bootstrap action that installs the sync agent JAR into `/usr/lib/hive/auxlib/`
   - `hive.metastore.event.listeners` configured to the sync agent class
   - Iceberg support enabled via `spark-defaults` and `iceberg-defaults`
   - 60-second idle auto-termination so the cluster shuts down shortly after steps complete
3. Spark SQL steps run on the cluster to create tables, insert data, alter schemas, and drop tables
4. The sync agent (running as an HMS listener) pushes events to the Glue Data Catalog
5. A Python validation script checks every synced table via the Glue API and runs Athena queries to confirm readability
6. On exit (pass or fail), the cleanup routine deletes the CloudFormation stack, Glue catalog entries, and S3 test data

### Running standalone (without Maven)

```bash
cd integration-tests

# If you already have an EMR cluster with the sync agent installed:
export EMR_CLUSTER_ID=j-XXXXXXXXXXXXX
export TEST_S3_BUCKET=my-test-bucket
./run-tests.sh

# Or run the full lifecycle (provision → test → teardown):
export TEST_S3_BUCKET=my-test-bucket
export SUBNET_ID=subnet-0abc123def456
./run-integ-tests.sh
```

## Load Tests

The project includes a load testing framework that generates high-volume HMS events via Spark SQL on EMR, waits for the sync agent to process them, and validates correctness and performance against the Glue Data Catalog and CloudWatch Metrics.

### Scenarios

| Scenario | DBs | Tables/DB | Partitions/Table | Total Ops | Purpose |
|---|---|---|---|---|---|
| `burst-create` | 1 | 100 | 0 | 100 | CreateTable throughput, batch merging |
| `partition-heavy` | 1 | 5 | 200 | ~1,005 | BatchCreatePartition batching, Glue API limits |
| `multi-db` | 10 | 10 | 10 | ~1,100 | CreateDatabase + cross-DB parallelism |
| `mixed-ops` | 1 | 50 | 20 | ~150 | Create → Alter → Drop interleaving |
| `sustained` | 1 | 500 | 5 | ~3,000 | Queue depth, memory, long-running stability |

### Prerequisites

- AWS CLI configured with credentials that can create EMR clusters, Glue catalog entries, and access CloudWatch Metrics/Logs
- Python 3.6+ with `boto3` installed
- An S3 bucket for test artifacts
- A VPC subnet with EMR access
- The sync agent fat JAR built locally: `mvn package assembly:assembly -DskipTests`

### Running via Maven

```bash
mvn verify -Pload-test \
  -Dtest.s3.bucket=my-bucket \
  -Dtest.subnet.id=subnet-abc123 \
  -Dtest.scenario=burst-create \
  -Dtest.batch.window.seconds=10
```

Run multiple scenarios sequentially:

```bash
mvn verify -Pload-test \
  -Dtest.s3.bucket=my-bucket \
  -Dtest.subnet.id=subnet-abc123 \
  -Dtest.scenario=burst-create,partition-heavy,sustained
```

| Property | Required | Default | Description |
|---|---|---|---|
| `test.s3.bucket` | Yes | — | S3 bucket for artifacts, DDL, and reports |
| `test.subnet.id` | Yes | — | VPC subnet for the EMR cluster |
| `test.aws.region` | No | `us-east-1` | AWS region |
| `test.scenario` | No | `burst-create` | Comma-separated scenario names |
| `test.batch.window.seconds` | No | `10` | Sync agent batch window in seconds |

### Running standalone

```bash
export TEST_S3_BUCKET=my-bucket
export SUBNET_ID=subnet-abc123
export AWS_REGION=us-east-1
export SCENARIO=burst-create          # or comma-separated: burst-create,sustained
export BATCH_WINDOW_SECONDS=10        # optional, default 10

bash integration-tests/load-test/run-load-test.sh
```

### Running individual scripts

Generate DDL without running the full test:

```bash
python3 integration-tests/load-test/generate-load.py \
  --scenario partition-heavy \
  --output-dir /tmp/ddl \
  --s3-bucket my-bucket
```

Run validation against an existing database (after a manual test run):

```bash
python3 integration-tests/load-test/validate-load.py \
  --database load_test_db_0 \
  --region us-east-1 \
  --scenario burst-create \
  --expected-tables 100 \
  --expected-partitions 0
```

### What happens during a load test run

1. The sync agent fat JAR, bootstrap script, and Spark SQL wrapper are uploaded to S3
2. `generate-load.py` creates parameterized DDL files for the selected scenario
3. A CloudFormation stack deploys an EMR cluster with the sync agent installed and metrics enabled
4. EMR steps execute the Spark SQL DDL, triggering HMS events that the sync agent processes
5. After a wait period (proportional to the batch window), `validate-load.py` checks:
   - 100% sync completeness — every expected table and partition exists in the Glue Data Catalog
   - No CWL errors — no `DISALLOWED` or `ERROR` entries in the `HIVE_METADATA_SYNC` log group
   - Sync lag within threshold — average `SyncLagMs` does not exceed 300,000 ms
6. Each scenario produces a JSON report uploaded to `s3://<bucket>/load-test/reports/<scenario>-report.json`
7. On exit (pass or fail), cleanup deletes the CloudFormation stack, Glue databases/tables, and S3 test data

### Pass/fail criteria

A scenario passes when all of the following hold:

1. 100% sync completeness — every table and partition created by the DDL exists in the Glue Data Catalog
2. No CWL errors — no `DISALLOWED` or `ERROR` entries in CloudWatch Logs
3. Sync lag within threshold — average `SyncLagMs` ≤ 300,000 ms (configurable via `--sync-lag-threshold-ms`)

The overall load test exits `0` only if every selected scenario passes.

## Project structure

```
integration-tests/
├── cfn/
│   └── emr-integ-test.yaml          # CloudFormation template for EMR cluster
├── emr-steps/
│   ├── 01_create_hive_tables.sql     # Parquet, ORC, CSV external tables
│   ├── 02_create_iceberg_tables.sql  # Iceberg table scenarios
│   ├── 03_alter_and_drop.sql         # ALTER TABLE, DROP PARTITION
│   └── 04_drop_table.sql            # DROP TABLE (runs after sync window)
├── load-test/
│   ├── generate-load.py              # DDL generator for load scenarios
│   ├── validate-load.py              # Sync completeness + metrics validator
│   ├── run-load-test.sh              # Full load test orchestrator
│   └── README.md                     # Detailed load test documentation
├── scripts/
│   ├── bootstrap-install-agent.sh    # EMR bootstrap to install the JAR
│   └── run-spark-sql.sh              # Spark SQL wrapper for EMR steps
├── validate/
│   └── validate_sync.py             # GDC metadata + Athena readability checks
├── run-integ-tests.sh               # Full lifecycle orchestrator
├── run-tests.sh                     # Step submission + validation (existing cluster)
├── test-scenarios.md                # Detailed test case descriptions
└── README.md                        # Integration test documentation
```

----

## Future Enhancements
* Support Hive Statistics to GDC Statistics
* 

Apache 2.0 Software License

see [LICENSE](LICENSE) for details
