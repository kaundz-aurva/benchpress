from __future__ import annotations

DEFAULT_CSV_DIRNAME = "csv"

RUNS_CSV_FILENAME = "runs.csv"
AGGREGATES_CSV_FILENAME = "aggregates.csv"
OVERHEAD_CSV_FILENAME = "overhead.csv"
FAILURES_CSV_FILENAME = "failures.csv"
HOST_RUNS_CSV_FILENAME = "host_runs.csv"
HOST_AGGREGATES_CSV_FILENAME = "host_aggregates.csv"
HOST_OVERHEAD_CSV_FILENAME = "host_overhead.csv"
HOST_SAMPLES_CSV_FILENAME = "host_samples.csv"
HOST_METRICS_CACHE_FILENAME = "host_metrics_cache.json"

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
HOST_NON_AGGREGATE_METRIC_KEYS = ("sample_count",)
HOST_METRIC_KEYS = (
    "sample_count",
    "total_cpu_percent_avg",
    "total_cpu_percent_max",
    "sql_cpu_percent_avg",
    "sql_cpu_percent_max",
    "available_memory_mb_avg",
    "available_memory_mb_min",
    "memory_used_mb_avg",
    "memory_used_mb_max",
    "memory_used_percent_avg",
    "memory_used_percent_max",
    "sql_working_set_mb_avg",
    "sql_working_set_mb_max",
)
HOST_SAMPLE_FIELD_KEYS = (
    "total_cpu_percent",
    "sql_cpu_percent",
    "available_memory_mb",
    "memory_used_mb",
    "memory_used_percent",
    "sql_working_set_mb",
)

REPORT_TITLE = "Benchpress Benchmark Report"
SECTION_OVERVIEW = "Benchmark Overview"
SECTION_MATRIX = "Run Matrix Summary"
SECTION_THROUGHPUT = "Per-VU Throughput Comparison"
SECTION_HOST_METRICS = "CPU and Memory Metrics"
SECTION_RUNS = "Per-Run Results"
SECTION_FAILURES = "Failure Diagnostics"
SECTION_ARTIFACTS = "Artifact Notes"

MISSING_VALUE = "not available"
