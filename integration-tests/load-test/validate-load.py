#!/usr/bin/env python3
"""
Load test validation script for Hive Glue Catalog Sync Agent.

Validates sync completeness and performance after a load test run by:
- Polling GDC for table/partition completeness
- Querying CloudWatch Logs for DISALLOWED/ERROR entries
- Querying CloudWatch Metrics for SyncLagMs, OperationSuccess, OperationFailure, ThrottleCount
- Producing a JSON summary report with pass/fail verdict

Usage:
    python3 validate-load.py --database <db> --region <region>
           --scenario <name> --expected-tables N --expected-partitions N
           [--metrics-namespace <ns>] [--sync-lag-threshold-ms N]
           [--s3-output <s3-path>]

Exit codes: 0 = pass, 1 = fail
"""

import argparse
import json
import sys
import time


# ---------------------------------------------------------------------------
# Pure validation functions (importable for testing without AWS)
# ---------------------------------------------------------------------------


def compute_sync_completeness(expected_tables, actual_tables):
    """Compute sync completeness as |expected ∩ actual| / |expected| × 100.

    Args:
        expected_tables: Set (or list) of expected table names.
        actual_tables: Set (or list) of actual table names found in GDC.

    Returns:
        Float percentage (0.0 - 100.0). Returns 100.0 if expected is empty.
    """
    expected_set = set(expected_tables)
    actual_set = set(actual_tables)
    if len(expected_set) == 0:
        return 100.0
    intersection = expected_set & actual_set
    return (len(intersection) / len(expected_set)) * 100.0


def detect_cwl_errors(log_entries):
    """Identify CloudWatch Logs entries containing DISALLOWED or ERROR status.

    Args:
        log_entries: List of log entry strings.

    Returns:
        Dict with 'errors' (list of entries containing ERROR)
        and 'disallowed' (list of entries containing DISALLOWED).
    """
    errors = []
    disallowed = []
    for entry in log_entries:
        if "DISALLOWED" in entry:
            disallowed.append(entry)
        if "ERROR" in entry:
            errors.append(entry)
    return {"errors": errors, "disallowed": disallowed}


def check_sync_lag(avg_lag_ms, threshold_ms):
    """Check whether average sync lag is within the acceptable threshold.

    Args:
        avg_lag_ms: Average sync lag in milliseconds.
        threshold_ms: Maximum acceptable sync lag in milliseconds.

    Returns:
        True if lag <= threshold (pass), False if lag > threshold (fail).
    """
    return avg_lag_ms <= threshold_ms


def build_validation_report(
    scenario,
    total_ops,
    completeness_pct,
    tables_expected,
    tables_found,
    partitions_expected,
    partitions_found,
    avg_sync_lag,
    p99_sync_lag,
    success_count,
    failure_count,
    throttle_count,
    cwl_errors,
    cwl_disallowed,
    failure_reasons,
):
    """Build a structured validation summary report.

    Args:
        scenario: Scenario name string.
        total_ops: Total number of operations.
        completeness_pct: Sync completeness percentage (0-100).
        tables_expected: Number of tables expected.
        tables_found: Number of tables found in GDC.
        partitions_expected: Number of partitions expected.
        partitions_found: Number of partitions found in GDC.
        avg_sync_lag: Average SyncLagMs value.
        p99_sync_lag: P99 SyncLagMs value.
        success_count: Total OperationSuccess count.
        failure_count: Total OperationFailure count.
        throttle_count: Total ThrottleCount.
        cwl_errors: List of CWL error entries.
        cwl_disallowed: List of CWL disallowed entries.
        failure_reasons: List of failure reason strings.

    Returns:
        Dict containing all report fields including pass/fail verdict.
    """
    passed = len(failure_reasons) == 0
    return {
        "scenario": scenario,
        "total_operations": total_ops,
        "sync_completeness_pct": completeness_pct,
        "tables_expected": tables_expected,
        "tables_found": tables_found,
        "partitions_expected": partitions_expected,
        "partitions_found": partitions_found,
        "avg_sync_lag_ms": avg_sync_lag,
        "p99_sync_lag_ms": p99_sync_lag,
        "operation_success_count": success_count,
        "operation_failure_count": failure_count,
        "throttle_count": throttle_count,
        "cwl_errors": cwl_errors,
        "cwl_disallowed": cwl_disallowed,
        "pass": passed,
        "failure_reasons": failure_reasons,
    }


# ---------------------------------------------------------------------------
# AWS interaction functions (used by CLI, not tested in property tests)
# ---------------------------------------------------------------------------

DEFAULT_SYNC_LAG_THRESHOLD_MS = 300000
DEFAULT_METRICS_NAMESPACE = "HiveGlueCatalogSync"
POLL_INTERVAL_SECONDS = 10
POLL_TIMEOUT_SECONDS = 600


def _create_glue_client(region):
    """Create a Glue client for the given region."""
    import boto3

    return boto3.client("glue", region_name=region)


def _create_cw_client(region):
    """Create a CloudWatch client for the given region."""
    import boto3

    return boto3.client("cloudwatch", region_name=region)


def _create_logs_client(region):
    """Create a CloudWatch Logs client for the given region."""
    import boto3

    return boto3.client("logs", region_name=region)


def poll_gdc_tables(glue_client, database, expected_count, timeout=POLL_TIMEOUT_SECONDS):
    """Poll GDC until expected number of tables appear or timeout."""
    start = time.time()
    tables = []
    while time.time() - start < timeout:
        try:
            paginator = glue_client.get_paginator("get_tables")
            tables = []
            for page in paginator.paginate(DatabaseName=database):
                tables.extend([t["Name"] for t in page.get("TableList", [])])
            if len(tables) >= expected_count:
                return tables
        except Exception as e:
            print(f"Warning: GDC poll error: {e}")
        time.sleep(POLL_INTERVAL_SECONDS)
    return tables


def poll_gdc_partitions(glue_client, database, table_name):
    """Get all partitions for a table from GDC."""
    partitions = []
    try:
        paginator = glue_client.get_paginator("get_partitions")
        for page in paginator.paginate(DatabaseName=database, TableName=table_name):
            partitions.extend(page.get("Partitions", []))
    except Exception as e:
        print(f"Warning: partition poll error for {database}.{table_name}: {e}")
    return partitions


def query_cwl_entries(logs_client, log_group, start_time_ms, end_time_ms):
    """Query CloudWatch Logs for entries in the given time range."""
    entries = []
    try:
        paginator = logs_client.get_paginator("filter_log_events")
        for page in paginator.paginate(
            logGroupName=log_group,
            startTime=start_time_ms,
            endTime=end_time_ms,
        ):
            for event in page.get("events", []):
                entries.append(event.get("message", ""))
    except Exception as e:
        print(f"Warning: CWL query error: {e}")
    return entries


def query_cw_metric_statistics(cw_client, namespace, metric_name, start_time, end_time, period=300):
    """Query CloudWatch Metrics for statistics on a given metric."""
    try:
        response = cw_client.get_metric_statistics(
            Namespace=namespace,
            MetricName=metric_name,
            StartTime=start_time,
            EndTime=end_time,
            Period=period,
            Statistics=["Average", "Sum", "Maximum"],
            ExtendedStatistics=["p99"],
        )
        return response.get("Datapoints", [])
    except Exception as e:
        print(f"Warning: CW Metrics query error for {metric_name}: {e}")
        return []


def run_validation(args):
    """Run the full validation pipeline against AWS."""
    import datetime

    glue_client = _create_glue_client(args.region)
    cw_client = _create_cw_client(args.region)
    logs_client = _create_logs_client(args.region)

    failure_reasons = []

    # 1. Poll GDC for table completeness
    print(f"Polling GDC for tables in database '{args.database}'...")
    actual_tables = poll_gdc_tables(glue_client, args.database, args.expected_tables)
    expected_table_names = {f"load_test_tbl_{i}" for i in range(args.expected_tables)}
    actual_table_names = set(actual_tables)

    completeness_pct = compute_sync_completeness(expected_table_names, actual_table_names)
    tables_found = len(actual_table_names & expected_table_names)

    if completeness_pct < 100.0:
        failure_reasons.append(
            f"Sync completeness {completeness_pct:.1f}% < 100%: "
            f"missing {args.expected_tables - tables_found} tables"
        )

    # 2. Poll GDC for partition completeness
    total_partitions_found = 0
    for tbl_name in actual_table_names & expected_table_names:
        partitions = poll_gdc_partitions(glue_client, args.database, tbl_name)
        total_partitions_found += len(partitions)

    if total_partitions_found < args.expected_partitions:
        failure_reasons.append(
            f"Partition completeness: found {total_partitions_found}/{args.expected_partitions}"
        )

    # 3. Query CWL for errors
    end_time = datetime.datetime.utcnow()
    start_time = end_time - datetime.timedelta(hours=1)
    start_ms = int(start_time.timestamp() * 1000)
    end_ms = int(end_time.timestamp() * 1000)

    log_entries = query_cwl_entries(
        logs_client, "HIVE_METADATA_SYNC", start_ms, end_ms
    )
    cwl_result = detect_cwl_errors(log_entries)

    if cwl_result["errors"]:
        failure_reasons.append(f"Found {len(cwl_result['errors'])} CWL ERROR entries")
    if cwl_result["disallowed"]:
        failure_reasons.append(
            f"Found {len(cwl_result['disallowed'])} CWL DISALLOWED entries"
        )

    # 4. Query CW Metrics
    namespace = args.metrics_namespace
    avg_sync_lag = 0.0
    p99_sync_lag = 0.0
    success_count = 0
    failure_count = 0
    throttle_count = 0

    sync_lag_data = query_cw_metric_statistics(
        cw_client, namespace, "SyncLagMs", start_time, end_time
    )
    if sync_lag_data:
        avg_sync_lag = sum(d.get("Average", 0) for d in sync_lag_data) / len(sync_lag_data)
        p99_values = [d.get("ExtendedStatistics", {}).get("p99", 0) for d in sync_lag_data]
        p99_sync_lag = max(p99_values) if p99_values else 0.0

    success_data = query_cw_metric_statistics(
        cw_client, namespace, "OperationSuccess", start_time, end_time
    )
    success_count = int(sum(d.get("Sum", 0) for d in success_data))

    failure_data = query_cw_metric_statistics(
        cw_client, namespace, "OperationFailure", start_time, end_time
    )
    failure_count = int(sum(d.get("Sum", 0) for d in failure_data))

    throttle_data = query_cw_metric_statistics(
        cw_client, namespace, "ThrottleCount", start_time, end_time
    )
    throttle_count = int(sum(d.get("Sum", 0) for d in throttle_data))

    # 5. Check sync lag threshold
    if avg_sync_lag > 0 and not check_sync_lag(avg_sync_lag, args.sync_lag_threshold_ms):
        failure_reasons.append(
            f"Avg sync lag {avg_sync_lag:.1f}ms exceeds threshold {args.sync_lag_threshold_ms}ms"
        )

    # 6. Build report
    total_ops = args.expected_tables + args.expected_partitions
    report = build_validation_report(
        scenario=args.scenario,
        total_ops=total_ops,
        completeness_pct=completeness_pct,
        tables_expected=args.expected_tables,
        tables_found=tables_found,
        partitions_expected=args.expected_partitions,
        partitions_found=total_partitions_found,
        avg_sync_lag=avg_sync_lag,
        p99_sync_lag=p99_sync_lag,
        success_count=success_count,
        failure_count=failure_count,
        throttle_count=throttle_count,
        cwl_errors=cwl_result["errors"],
        cwl_disallowed=cwl_result["disallowed"],
        failure_reasons=failure_reasons,
    )

    # 7. Output report
    report_json = json.dumps(report, indent=2)
    print(report_json)

    if args.s3_output:
        try:
            import boto3

            s3 = boto3.client("s3", region_name=args.region)
            # Parse s3://bucket/key
            parts = args.s3_output.replace("s3://", "").split("/", 1)
            bucket = parts[0]
            key = parts[1] if len(parts) > 1 else "validation-report.json"
            s3.put_object(Bucket=bucket, Key=key, Body=report_json)
            print(f"Report uploaded to {args.s3_output}")
        except Exception as e:
            print(f"Warning: Failed to upload report to S3: {e}")

    return report


def main():
    parser = argparse.ArgumentParser(
        description="Validate load test results for Hive Glue Catalog Sync Agent"
    )
    parser.add_argument("--database", required=True, help="Glue database name to validate")
    parser.add_argument("--region", required=True, help="AWS region")
    parser.add_argument("--scenario", required=True, help="Load test scenario name")
    parser.add_argument(
        "--expected-tables", required=True, type=int, help="Expected number of tables"
    )
    parser.add_argument(
        "--expected-partitions", required=True, type=int, help="Expected number of partitions"
    )
    parser.add_argument(
        "--metrics-namespace",
        default=DEFAULT_METRICS_NAMESPACE,
        help=f"CloudWatch Metrics namespace (default: {DEFAULT_METRICS_NAMESPACE})",
    )
    parser.add_argument(
        "--sync-lag-threshold-ms",
        type=float,
        default=DEFAULT_SYNC_LAG_THRESHOLD_MS,
        help=f"Max acceptable avg sync lag in ms (default: {DEFAULT_SYNC_LAG_THRESHOLD_MS})",
    )
    parser.add_argument(
        "--s3-output", default=None, help="S3 path to upload the JSON report"
    )

    args = parser.parse_args()

    report = run_validation(args)

    if report["pass"]:
        print("\n✅ VALIDATION PASSED")
        sys.exit(0)
    else:
        print("\n❌ VALIDATION FAILED")
        for reason in report["failure_reasons"]:
            print(f"  - {reason}")
        sys.exit(1)


if __name__ == "__main__":
    main()
