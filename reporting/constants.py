from __future__ import annotations

DEFAULT_CSV_DIRNAME = "csv"

RUNS_CSV_FILENAME = "runs.csv"
AGGREGATES_CSV_FILENAME = "aggregates.csv"
OVERHEAD_CSV_FILENAME = "overhead.csv"
FAILURES_CSV_FILENAME = "failures.csv"

SUCCESS_STATUS = "success"
AUDIT_OFF_MODE = "audit_off"
AUDIT_ON_MODE = "audit_on"

WORKLOAD_SUMMARY_KEY = "workload"
HAMMERDB_STDOUT_FILENAME = "hammerdb_stdout.txt"
ARTIFACT_FALLBACK_FILENAMES = (HAMMERDB_STDOUT_FILENAME,)

DEFAULT_METRIC_KEYS = (
    "tpm",
    "nopm",
    "latency_ms",
    "duration_seconds",
    "benchmark_status",
    "virtual_users",
)
THROUGHPUT_METRIC_KEYS = ("tpm", "nopm")
NON_AGGREGATE_METRIC_KEYS = ("virtual_users",)

REPORT_TITLE = "Benchpress Benchmark Report"
SECTION_OVERVIEW = "Benchmark Overview"
SECTION_MATRIX = "Run Matrix Summary"
SECTION_THROUGHPUT = "Per-VU Throughput Comparison"
SECTION_RUNS = "Per-Run Results"
SECTION_FAILURES = "Failure Diagnostics"
SECTION_ARTIFACTS = "Artifact Notes"

MISSING_VALUE = "not available"
