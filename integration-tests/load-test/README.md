# Load Testing Framework

End-to-end load tests for the Hive Glue Catalog Sync Agent. Generates high-volume HMS events via Spark SQL on EMR, waits for sync completion, and validates correctness and performance using the Glue Data Catalog and CloudWatch Metrics.

## Prerequisites

- AWS CLI configured with appropriate credentials
- Python 3.6+
- An S3 bucket for test artifacts
- A VPC subnet with EMR access
- The sync agent fat JAR built locally (`mvn package assembly:assembly -DskipTests`)

## Quick Start

```bash
mvn verify -Pload-test \
  -Dtest.s3.bucket=my-bucket \
  -Dtest.subnet.id=subnet-abc123 \
  -Dtest.aws.region=us-east-1 \
  -Dtest.scenario=burst-create \
  -Dtest.batch.window.seconds=10
```

## Scenarios

| Scenario | DBs | Tables/DB | Partitions/Table | Total Ops | Purpose |
|---|---|---|---|---|---|
| `burst-create` | 1 | 100 | 0 | 100 | CreateTable throughput, batch merging |
| `partition-heavy` | 1 | 5 | 200 | ~1,005 | BatchCreatePartition batching, Glue API limits |
| `multi-db` | 10 | 10 | 10 | ~1,100 | CreateDatabase + cross-DB parallelism |
| `mixed-ops` | 1 | 50 | 20 | ~150 | Create → Alter → Drop interleaving |
| `sustained` | 1 | 500 | 5 | ~3,000 | Queue depth, memory, long-running stability |

## Maven Commands

Run a single scenario:

```bash
mvn verify -Pload-test \
  -Dtest.s3.bucket=my-bucket \
  -Dtest.subnet.id=subnet-abc123 \
  -Dtest.scenario=burst-create
```

Run multiple scenarios sequentially:

```bash
mvn verify -Pload-test \
  -Dtest.s3.bucket=my-bucket \
  -Dtest.subnet.id=subnet-abc123 \
  -Dtest.scenario=burst-create,partition-heavy
```

Run with a custom batch window (seconds):

```bash
mvn verify -Pload-test \
  -Dtest.s3.bucket=my-bucket \
  -Dtest.subnet.id=subnet-abc123 \
  -Dtest.scenario=sustained \
  -Dtest.batch.window.seconds=30
```

## Configuration

### Maven Properties

| Property | Required | Default | Description |
|---|---|---|---|
| `test.s3.bucket` | Yes | — | S3 bucket for artifacts, DDL, and reports |
| `test.subnet.id` | Yes | — | VPC subnet for the EMR cluster |
| `test.aws.region` | No | `us-east-1` | AWS region |
| `test.scenario` | No | `burst-create` | Comma-separated scenario names |
| `test.batch.window.seconds` | No | `10` | Sync agent batch window in seconds |

### Environment Variables (used by `run-load-test.sh`)

| Variable | Required | Default | Description |
|---|---|---|---|
| `TEST_S3_BUCKET` | Yes | — | S3 bucket for test artifacts |
| `SUBNET_ID` | Yes | — | VPC subnet for EMR cluster |
| `AWS_REGION` | No | `us-east-1` | AWS region |
| `SCENARIO` | No | `burst-create` | Comma-separated scenario names |
| `BATCH_WINDOW_SECONDS` | No | `10` | Sync agent batch window |
| `EMR_RELEASE` | No | `emr-7.1.0` | EMR release label |
| `GLUE_CATALOG_ID` | No | Account default | Glue Data Catalog ID |

## Pass/Fail Criteria

A scenario passes when all of the following hold:

1. **100% sync completeness** — every table and partition created by the DDL exists in the Glue Data Catalog.
2. **No CWL errors** — no `DISALLOWED` or `ERROR` entries in the `HIVE_METADATA_SYNC` CloudWatch Logs group.
3. **Sync lag within threshold** — average `SyncLagMs` does not exceed 300,000 ms (configurable via `--sync-lag-threshold-ms`).

The overall load test exits `0` only if every selected scenario passes.

## Scripts

| Script | Description |
|---|---|
| `generate-load.py` | Generates Spark SQL DDL files from scenario parameters |
| `validate-load.py` | Validates sync completeness, queries CW Metrics/Logs, produces JSON report |
| `run-load-test.sh` | Orchestrates the full lifecycle: generate → upload → deploy → submit → validate → cleanup |

### Running Scripts Directly

Generate DDL without running the full test:

```bash
python3 generate-load.py \
  --scenario partition-heavy \
  --output-dir /tmp/ddl \
  --s3-bucket my-bucket
```

Run validation against an existing database:

```bash
python3 validate-load.py \
  --database load_test_db_0 \
  --region us-east-1 \
  --scenario burst-create \
  --expected-tables 100 \
  --expected-partitions 0
```

## Lifecycle

1. **Build** — `mvn package assembly:assembly -DskipTests` produces the fat JAR.
2. **Generate DDL** — `generate-load.py` creates `.sql` files for the selected scenario.
3. **Upload** — JAR, bootstrap script, and DDL are uploaded to S3.
4. **Deploy** — CloudFormation creates an EMR cluster with the sync agent installed and metrics enabled.
5. **Submit** — EMR steps execute the Spark SQL DDL, triggering HMS events.
6. **Wait** — The orchestrator waits for the sync agent to process all events.
7. **Validate** — `validate-load.py` checks GDC completeness, CWL errors, and CW Metrics.
8. **Cleanup** — Glue databases/tables, the CFN stack, and S3 data are deleted (runs on exit, including failures).

## Validation Report

Each scenario produces a JSON report with the following fields:

```json
{
  "scenario": "burst-create",
  "total_operations": 100,
  "sync_completeness_pct": 100.0,
  "tables_expected": 100,
  "tables_found": 100,
  "partitions_expected": 0,
  "partitions_found": 0,
  "avg_sync_lag_ms": 1234.5,
  "p99_sync_lag_ms": 4567.8,
  "operation_success_count": 100,
  "operation_failure_count": 0,
  "throttle_count": 0,
  "cwl_errors": [],
  "cwl_disallowed": [],
  "pass": true,
  "failure_reasons": []
}
```

Reports are uploaded to `s3://<bucket>/load-test/reports/<scenario>-report.json`.
