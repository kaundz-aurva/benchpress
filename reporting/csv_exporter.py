from __future__ import annotations

import csv
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

from reporting.constants import (
    AGGREGATES_CSV_FILENAME,
    DEFAULT_METRIC_KEYS,
    FAILURES_CSV_FILENAME,
    HOST_AGGREGATES_CSV_FILENAME,
    HOST_METRIC_KEYS,
    HOST_OVERHEAD_CSV_FILENAME,
    HOST_RUNS_CSV_FILENAME,
    HOST_SAMPLE_FIELD_KEYS,
    HOST_SAMPLES_CSV_FILENAME,
    OVERHEAD_CSV_FILENAME,
    RUNS_CSV_FILENAME,
)
from reporting.models import AggregateRow, FailureRow, OverheadRow, ReportDocument, ReportRunRow


class CsvReportExporter:
    def write(self, document: ReportDocument, csv_dir: Path) -> dict[str, Path]:
        paths = {
            "runs": csv_dir / RUNS_CSV_FILENAME,
            "aggregates": csv_dir / AGGREGATES_CSV_FILENAME,
            "overhead": csv_dir / OVERHEAD_CSV_FILENAME,
            "failures": csv_dir / FAILURES_CSV_FILENAME,
            "host_runs": csv_dir / HOST_RUNS_CSV_FILENAME,
            "host_aggregates": csv_dir / HOST_AGGREGATES_CSV_FILENAME,
            "host_overhead": csv_dir / HOST_OVERHEAD_CSV_FILENAME,
            "host_samples": csv_dir / HOST_SAMPLES_CSV_FILENAME,
        }
        self._write_runs_csv(document.runs, paths["runs"])
        self._write_aggregates_csv(document.aggregates, paths["aggregates"])
        self._write_overhead_csv(document.overhead, paths["overhead"])
        self._write_failures_csv(document.failures, paths["failures"])
        self._write_host_runs_csv(document.runs, paths["host_runs"])
        self._write_aggregates_csv(
            document.host_aggregates,
            paths["host_aggregates"],
            HOST_METRIC_KEYS,
        )
        self._write_overhead_csv(document.host_overhead, paths["host_overhead"], HOST_METRIC_KEYS)
        self._write_host_samples_csv(document.runs, paths["host_samples"])
        return paths

    def _write_runs_csv(self, runs: tuple[ReportRunRow, ...], path: Path) -> None:
        fixed_headers = [
            "run_id",
            "benchmark_name",
            "workload_name",
            "workload_tool",
            "audit_mode",
            "virtual_users",
            "repetition",
            "status",
            "phase",
            "output_dir",
            "created_at",
            "updated_at",
            "summary_notes",
        ]
        metric_headers = _ordered_metric_keys(
            metric_key
            for run in runs
            for metric_key in run.workload_metrics
            if metric_key not in fixed_headers
        )
        with path.open("w", encoding="utf-8", newline="") as file_obj:
            writer = csv.DictWriter(file_obj, fieldnames=fixed_headers + metric_headers)
            writer.writeheader()
            for run in runs:
                row: dict[str, Any] = {
                    "run_id": run.run_id,
                    "benchmark_name": run.benchmark_name,
                    "workload_name": run.workload_name,
                    "workload_tool": run.workload_tool,
                    "audit_mode": run.audit_mode,
                    "virtual_users": run.virtual_users,
                    "repetition": run.repetition,
                    "status": run.status,
                    "phase": run.phase,
                    "output_dir": str(run.output_dir),
                    "created_at": run.created_at,
                    "updated_at": run.updated_at,
                    "summary_notes": run.summary_notes,
                }
                for key in metric_headers:
                    row[key] = _csv_value(run.workload_metrics.get(key))
                writer.writerow(row)

    def _write_aggregates_csv(
        self,
        aggregates: tuple[AggregateRow, ...],
        path: Path,
        preferred_order: tuple[str, ...] = DEFAULT_METRIC_KEYS,
    ) -> None:
        metric_keys = _ordered_metric_keys(
            (metric_key for aggregate in aggregates for metric_key in aggregate.metrics),
            preferred_order=preferred_order,
        )
        headers = ["audit_mode", "virtual_users", "run_count"]
        for key in metric_keys:
            headers.extend([f"{key}_count", f"{key}_mean", f"{key}_min", f"{key}_max"])
        with path.open("w", encoding="utf-8", newline="") as file_obj:
            writer = csv.DictWriter(file_obj, fieldnames=headers)
            writer.writeheader()
            for aggregate in aggregates:
                row: dict[str, Any] = {
                    "audit_mode": aggregate.audit_mode,
                    "virtual_users": aggregate.virtual_users,
                    "run_count": aggregate.run_count,
                }
                for key in metric_keys:
                    stats = aggregate.metrics.get(key)
                    if stats is None:
                        continue
                    row[f"{key}_count"] = stats.count
                    row[f"{key}_mean"] = _csv_value(stats.mean)
                    row[f"{key}_min"] = _csv_value(stats.minimum)
                    row[f"{key}_max"] = _csv_value(stats.maximum)
                writer.writerow(row)

    def _write_overhead_csv(
        self,
        overhead: tuple[OverheadRow, ...],
        path: Path,
        preferred_order: tuple[str, ...] = DEFAULT_METRIC_KEYS,
    ) -> None:
        metric_keys = _ordered_metric_keys(
            (row.metric_name for row in overhead),
            preferred_order=preferred_order,
        )
        by_vu: dict[int, dict[str, OverheadRow]] = defaultdict(dict)
        for row in overhead:
            by_vu[row.virtual_users][row.metric_name] = row
        headers = ["virtual_users"]
        for key in metric_keys:
            headers.extend(
                [
                    f"{key}_audit_off_mean",
                    f"{key}_audit_on_mean",
                    f"{key}_delta",
                    f"{key}_percent_change",
                ]
            )
        with path.open("w", encoding="utf-8", newline="") as file_obj:
            writer = csv.DictWriter(file_obj, fieldnames=headers)
            writer.writeheader()
            for vu in sorted(by_vu):
                output_row: dict[str, Any] = {"virtual_users": vu}
                for key in metric_keys:
                    row = by_vu[vu].get(key)
                    if row is None:
                        continue
                    output_row[f"{key}_audit_off_mean"] = _csv_value(row.audit_off_mean)
                    output_row[f"{key}_audit_on_mean"] = _csv_value(row.audit_on_mean)
                    output_row[f"{key}_delta"] = _csv_value(row.delta)
                    output_row[f"{key}_percent_change"] = _csv_value(row.percent_change)
                writer.writerow(output_row)

    def _write_failures_csv(self, failures: tuple[FailureRow, ...], path: Path) -> None:
        headers = [
            "run_id",
            "audit_mode",
            "virtual_users",
            "repetition",
            "phase",
            "status",
            "exception_type",
            "message",
        ]
        with path.open("w", encoding="utf-8", newline="") as file_obj:
            writer = csv.DictWriter(file_obj, fieldnames=headers)
            writer.writeheader()
            for failure in failures:
                writer.writerow(
                    {
                        "run_id": failure.run_id,
                        "audit_mode": failure.audit_mode,
                        "virtual_users": failure.virtual_users,
                        "repetition": failure.repetition,
                        "phase": failure.phase,
                        "status": failure.status,
                        "exception_type": failure.exception_type,
                        "message": failure.message,
                    }
                )

    def _write_host_runs_csv(self, runs: tuple[ReportRunRow, ...], path: Path) -> None:
        headers = [
            "run_id",
            "benchmark_name",
            "audit_mode",
            "virtual_users",
            "repetition",
            "status",
            "phase",
            "output_dir",
            "target_memory_gb",
            *HOST_METRIC_KEYS,
        ]
        with path.open("w", encoding="utf-8", newline="") as file_obj:
            writer = csv.DictWriter(file_obj, fieldnames=headers)
            writer.writeheader()
            for run in runs:
                row: dict[str, Any] = {
                    "run_id": run.run_id,
                    "benchmark_name": run.benchmark_name,
                    "audit_mode": run.audit_mode,
                    "virtual_users": run.virtual_users,
                    "repetition": run.repetition,
                    "status": run.status,
                    "phase": run.phase,
                    "output_dir": str(run.output_dir),
                    "target_memory_gb": run.target_memory_gb,
                }
                for key in HOST_METRIC_KEYS:
                    row[key] = _csv_value(run.host_metrics.get(key))
                writer.writerow(row)

    def _write_host_samples_csv(self, runs: tuple[ReportRunRow, ...], path: Path) -> None:
        headers = [
            "run_id",
            "audit_mode",
            "virtual_users",
            "repetition",
            "sample_index",
            "timestamp",
            *HOST_SAMPLE_FIELD_KEYS,
        ]
        with path.open("w", encoding="utf-8", newline="") as file_obj:
            writer = csv.DictWriter(file_obj, fieldnames=headers)
            writer.writeheader()
            for run in runs:
                for sample in run.host_samples:
                    writer.writerow(
                        {
                            "run_id": run.run_id,
                            "audit_mode": run.audit_mode,
                            "virtual_users": run.virtual_users,
                            "repetition": run.repetition,
                            "sample_index": sample.sample_index,
                            "timestamp": sample.timestamp,
                            "total_cpu_percent": _csv_value(sample.total_cpu_percent),
                            "sql_cpu_percent": _csv_value(sample.sql_cpu_percent),
                            "available_memory_mb": _csv_value(sample.available_memory_mb),
                            "memory_used_mb": _csv_value(sample.memory_used_mb),
                            "memory_used_percent": _csv_value(sample.memory_used_percent),
                            "sql_working_set_mb": _csv_value(sample.sql_working_set_mb),
                        }
                    )


def _ordered_metric_keys(
    metric_keys: Iterable[str],
    preferred_order: tuple[str, ...] = DEFAULT_METRIC_KEYS,
) -> list[str]:
    seen = {key for key in metric_keys if key}
    ordered = [key for key in preferred_order if key in seen]
    ordered.extend(sorted(seen.difference(preferred_order)))
    return ordered


def _csv_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.10g}"
    return str(value)
