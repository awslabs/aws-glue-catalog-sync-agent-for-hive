"""
Property-based tests for the load test validation script.

Uses pytest + hypothesis to verify correctness properties of validate-load.py
pure validation functions.
"""

import sys
import os

import pytest
from hypothesis import given, settings, assume
from hypothesis import strategies as st

# Ensure the load-test directory is importable
sys.path.insert(0, os.path.dirname(__file__))

# Import from validate-load.py (hyphenated filename requires importlib)
import importlib

validate_load = importlib.import_module("validate-load")
compute_sync_completeness = validate_load.compute_sync_completeness
detect_cwl_errors = validate_load.detect_cwl_errors
check_sync_lag = validate_load.check_sync_lag
build_validation_report = validate_load.build_validation_report


# --- Strategies ---

# Table names: simple alphanumeric identifiers
table_name_st = st.from_regex(r"[a-z][a-z0-9_]{2,30}", fullmatch=True)
table_name_set_st = st.frozensets(table_name_st, min_size=1, max_size=50)

# Log entry text without DISALLOWED or ERROR keywords
safe_log_text_st = st.text(
    alphabet=st.characters(
        whitelist_categories=("L", "N", "P", "Z"),
        blacklist_characters="\x00",
    ),
    min_size=1,
    max_size=100,
).filter(lambda s: "DISALLOWED" not in s and "ERROR" not in s)

# Positive floats for lag/threshold values
positive_float_st = st.floats(min_value=0.0, max_value=1e9, allow_nan=False, allow_infinity=False)
strict_positive_float_st = st.floats(min_value=0.001, max_value=1e9, allow_nan=False, allow_infinity=False)


# --- Property 11: Sync completeness validation ---
# Validates: Requirements 7.1


class TestSyncCompletenessValidation:
    """Property 11: Sync completeness validation.

    For any set of expected table names and actual GDC table names, the
    validator computes sync completeness as |expected ∩ actual| / |expected|
    × 100 and reports pass only when completeness is 100%.

    **Validates: Requirements 7.1**
    """

    @given(expected=table_name_set_st, actual=table_name_set_st)
    @settings(max_examples=200)
    def test_completeness_formula(self, expected, actual):
        """Completeness equals |expected ∩ actual| / |expected| × 100."""
        result = compute_sync_completeness(expected, actual)
        intersection = expected & actual
        expected_pct = (len(intersection) / len(expected)) * 100.0
        assert abs(result - expected_pct) < 1e-9

    @given(tables=table_name_set_st)
    @settings(max_examples=200)
    def test_full_match_is_100(self, tables):
        """When actual contains all expected tables, completeness is 100%."""
        result = compute_sync_completeness(tables, tables)
        assert result == 100.0

    @given(expected=table_name_set_st, extra=table_name_set_st)
    @settings(max_examples=200)
    def test_superset_actual_is_100(self, expected, extra):
        """When actual is a superset of expected, completeness is still 100%."""
        actual = expected | extra
        result = compute_sync_completeness(expected, actual)
        assert result == 100.0

    @given(expected=table_name_set_st)
    @settings(max_examples=200)
    def test_empty_actual_is_zero(self, expected):
        """When actual is empty, completeness is 0%."""
        result = compute_sync_completeness(expected, set())
        assert result == 0.0

    @given(expected=table_name_set_st, actual=table_name_set_st)
    @settings(max_examples=200)
    def test_partial_match_less_than_100(self, expected, actual):
        """When actual doesn't contain all expected, completeness < 100%
        (unless they happen to be a superset)."""
        result = compute_sync_completeness(expected, actual)
        if not expected.issubset(actual):
            assert result < 100.0
        else:
            assert result == 100.0


# --- Property 12: CWL error detection ---
# Validates: Requirements 7.2


class TestCWLErrorDetection:
    """Property 12: CWL error detection.

    For any set of CloudWatch Logs entries, the validator correctly
    identifies all entries containing DISALLOWED or ERROR status.

    **Validates: Requirements 7.2**
    """

    @given(safe_entries=st.lists(safe_log_text_st, min_size=0, max_size=20))
    @settings(max_examples=200)
    def test_no_false_positives(self, safe_entries):
        """Entries without ERROR or DISALLOWED are never flagged."""
        result = detect_cwl_errors(safe_entries)
        assert result["errors"] == []
        assert result["disallowed"] == []

    @given(
        safe_entries=st.lists(safe_log_text_st, min_size=0, max_size=10),
        error_count=st.integers(min_value=1, max_value=10),
        disallowed_count=st.integers(min_value=1, max_value=10),
    )
    @settings(max_examples=200)
    def test_detects_all_errors_and_disallowed(
        self, safe_entries, error_count, disallowed_count
    ):
        """All ERROR and DISALLOWED entries are detected."""
        error_entries = [f"ERROR: failure #{i}" for i in range(error_count)]
        disallowed_entries = [
            f"DISALLOWED table #{i}" for i in range(disallowed_count)
        ]
        all_entries = safe_entries + error_entries + disallowed_entries

        result = detect_cwl_errors(all_entries)
        assert len(result["errors"]) == error_count
        assert len(result["disallowed"]) == disallowed_count

    @given(
        prefix=safe_log_text_st,
        suffix=safe_log_text_st,
    )
    @settings(max_examples=200)
    def test_error_detected_anywhere_in_entry(self, prefix, suffix):
        """ERROR keyword is detected regardless of position in the entry."""
        entry = f"{prefix} ERROR {suffix}"
        result = detect_cwl_errors([entry])
        assert len(result["errors"]) == 1
        assert result["errors"][0] == entry

    @given(
        prefix=safe_log_text_st,
        suffix=safe_log_text_st,
    )
    @settings(max_examples=200)
    def test_disallowed_detected_anywhere_in_entry(self, prefix, suffix):
        """DISALLOWED keyword is detected regardless of position in the entry."""
        entry = f"{prefix} DISALLOWED {suffix}"
        result = detect_cwl_errors([entry])
        assert len(result["disallowed"]) == 1
        assert result["disallowed"][0] == entry

    @given(count=st.integers(min_value=1, max_value=10))
    @settings(max_examples=200)
    def test_entry_with_both_keywords(self, count):
        """An entry containing both ERROR and DISALLOWED appears in both lists."""
        entries = [f"ERROR DISALLOWED item {i}" for i in range(count)]
        result = detect_cwl_errors(entries)
        assert len(result["errors"]) == count
        assert len(result["disallowed"]) == count


# --- Property 13: Sync lag threshold enforcement ---
# Validates: Requirements 7.4


class TestSyncLagThresholdEnforcement:
    """Property 13: Sync lag threshold enforcement.

    For any sync lag value and configurable threshold, the validator
    reports pass when lag ≤ threshold and fail when lag > threshold.

    **Validates: Requirements 7.4**
    """

    @given(
        lag=positive_float_st,
        threshold=strict_positive_float_st,
    )
    @settings(max_examples=200)
    def test_lag_within_threshold_passes(self, lag, threshold):
        """When lag <= threshold, check_sync_lag returns True (pass)."""
        assume(lag <= threshold)
        assert check_sync_lag(lag, threshold) is True

    @given(
        lag=strict_positive_float_st,
        threshold=positive_float_st,
    )
    @settings(max_examples=200)
    def test_lag_exceeding_threshold_fails(self, lag, threshold):
        """When lag > threshold, check_sync_lag returns False (fail)."""
        assume(lag > threshold)
        assert check_sync_lag(lag, threshold) is False

    @given(value=positive_float_st)
    @settings(max_examples=200)
    def test_lag_equal_to_threshold_passes(self, value):
        """When lag == threshold exactly, check_sync_lag returns True (pass)."""
        assert check_sync_lag(value, value) is True

    @given(threshold=strict_positive_float_st)
    @settings(max_examples=200)
    def test_zero_lag_always_passes(self, threshold):
        """Zero lag always passes regardless of threshold."""
        assert check_sync_lag(0.0, threshold) is True


# --- Property 14: Validation report completeness ---
# Validates: Requirements 7.5

REQUIRED_REPORT_FIELDS = {
    "scenario",
    "total_operations",
    "sync_completeness_pct",
    "tables_expected",
    "tables_found",
    "partitions_expected",
    "partitions_found",
    "avg_sync_lag_ms",
    "p99_sync_lag_ms",
    "operation_success_count",
    "operation_failure_count",
    "throttle_count",
    "cwl_errors",
    "cwl_disallowed",
    "pass",
    "failure_reasons",
}


class TestValidationReportCompleteness:
    """Property 14: Validation report completeness.

    For any validation result, the produced summary report contains all
    required fields: scenario name, total operations, sync completeness
    percentage, average sync lag, p99 sync lag, operation success count,
    operation failure count, throttle count, and pass/fail verdict.

    **Validates: Requirements 7.5**
    """

    @given(
        scenario=st.text(min_size=1, max_size=30),
        total_ops=st.integers(min_value=0, max_value=100000),
        completeness_pct=st.floats(min_value=0.0, max_value=100.0, allow_nan=False),
        tables_expected=st.integers(min_value=0, max_value=10000),
        tables_found=st.integers(min_value=0, max_value=10000),
        partitions_expected=st.integers(min_value=0, max_value=100000),
        partitions_found=st.integers(min_value=0, max_value=100000),
        avg_sync_lag=positive_float_st,
        p99_sync_lag=positive_float_st,
        success_count=st.integers(min_value=0, max_value=100000),
        failure_count=st.integers(min_value=0, max_value=100000),
        throttle_count=st.integers(min_value=0, max_value=100000),
    )
    @settings(max_examples=200)
    def test_report_contains_all_required_fields(
        self,
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
    ):
        """Every report contains all required fields."""
        report = build_validation_report(
            scenario=scenario,
            total_ops=total_ops,
            completeness_pct=completeness_pct,
            tables_expected=tables_expected,
            tables_found=tables_found,
            partitions_expected=partitions_expected,
            partitions_found=partitions_found,
            avg_sync_lag=avg_sync_lag,
            p99_sync_lag=p99_sync_lag,
            success_count=success_count,
            failure_count=failure_count,
            throttle_count=throttle_count,
            cwl_errors=[],
            cwl_disallowed=[],
            failure_reasons=[],
        )
        assert set(report.keys()) == REQUIRED_REPORT_FIELDS

    @given(
        scenario=st.text(min_size=1, max_size=30),
        total_ops=st.integers(min_value=0, max_value=10000),
        failure_reasons=st.lists(st.text(min_size=1, max_size=50), min_size=0, max_size=5),
    )
    @settings(max_examples=200)
    def test_pass_verdict_matches_failure_reasons(self, scenario, total_ops, failure_reasons):
        """Report 'pass' is True iff failure_reasons is empty."""
        report = build_validation_report(
            scenario=scenario,
            total_ops=total_ops,
            completeness_pct=100.0,
            tables_expected=10,
            tables_found=10,
            partitions_expected=0,
            partitions_found=0,
            avg_sync_lag=0.0,
            p99_sync_lag=0.0,
            success_count=total_ops,
            failure_count=0,
            throttle_count=0,
            cwl_errors=[],
            cwl_disallowed=[],
            failure_reasons=failure_reasons,
        )
        if len(failure_reasons) == 0:
            assert report["pass"] is True
        else:
            assert report["pass"] is False

    @given(
        scenario=st.text(min_size=1, max_size=30),
        total_ops=st.integers(min_value=0, max_value=10000),
        completeness_pct=st.floats(min_value=0.0, max_value=100.0, allow_nan=False),
        avg_sync_lag=positive_float_st,
        p99_sync_lag=positive_float_st,
        success_count=st.integers(min_value=0, max_value=10000),
        failure_count=st.integers(min_value=0, max_value=10000),
        throttle_count=st.integers(min_value=0, max_value=10000),
    )
    @settings(max_examples=200)
    def test_report_values_match_inputs(
        self,
        scenario,
        total_ops,
        completeness_pct,
        avg_sync_lag,
        p99_sync_lag,
        success_count,
        failure_count,
        throttle_count,
    ):
        """Report field values match the inputs provided."""
        report = build_validation_report(
            scenario=scenario,
            total_ops=total_ops,
            completeness_pct=completeness_pct,
            tables_expected=50,
            tables_found=45,
            partitions_expected=100,
            partitions_found=90,
            avg_sync_lag=avg_sync_lag,
            p99_sync_lag=p99_sync_lag,
            success_count=success_count,
            failure_count=failure_count,
            throttle_count=throttle_count,
            cwl_errors=["err1"],
            cwl_disallowed=["bl1"],
            failure_reasons=["reason"],
        )
        assert report["scenario"] == scenario
        assert report["total_operations"] == total_ops
        assert report["sync_completeness_pct"] == completeness_pct
        assert report["avg_sync_lag_ms"] == avg_sync_lag
        assert report["p99_sync_lag_ms"] == p99_sync_lag
        assert report["operation_success_count"] == success_count
        assert report["operation_failure_count"] == failure_count
        assert report["throttle_count"] == throttle_count
        assert report["tables_expected"] == 50
        assert report["tables_found"] == 45
        assert report["partitions_expected"] == 100
        assert report["partitions_found"] == 90
        assert report["cwl_errors"] == ["err1"]
        assert report["cwl_disallowed"] == ["bl1"]
