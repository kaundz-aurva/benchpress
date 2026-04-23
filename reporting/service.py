from __future__ import annotations

import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any, Iterable

from reporting.constants import (
    ARTIFACT_FALLBACK_FILENAMES,
    AUDIT_OFF_MODE,
    AUDIT_ON_MODE,
    HOST_METRICS_CACHE_FILENAME,
    HOST_NON_AGGREGATE_METRIC_KEYS,
    NON_AGGREGATE_METRIC_KEYS,
    SUCCESS_STATUS,
    THROUGHPUT_METRIC_KEYS,
    WORKLOAD_SUMMARY_KEY,
)
from reporting.dto import ReportGenerationRequest, ReportGenerationResult
from reporting.host_metrics import HostMetricsCache, load_host_metrics
from reporting.models import (
    AggregateRow,
    FailureRow,
    MetricStats,
    OverheadRow,
    ReportDocument,
    ReportRunRow,
    ReportSourceRun,
)
from reporting.output_writer import ReportOutputWriter
from reporting.repository import ReportingRepository


class BenchmarkReportService:
    def generate(self, request: ReportGenerationRequest) -> ReportGenerationResult:
        return ReportOutputWriter().write(request, self.build_document(request))

    def build_document(self, request: ReportGenerationRequest) -> ReportDocument:
        with ReportingRepository(request.db_path) as repository:
            source_runs = repository.list_runs()

        host_metrics_cache = HostMetricsCache(
            request.resolved_csv_dir / HOST_METRICS_CACHE_FILENAME
        )
        runs = tuple(
            self._build_run_row(
                source_run,
                request.include_artifact_fallback,
                request.resolved_artifact_root,
                host_metrics_cache,
            )
            for source_run in source_runs
        )
        host_metrics_cache.save()
        aggregates = tuple(self._aggregate_runs(runs))
        overhead = tuple(self._calculate_overhead(aggregates))
        host_aggregates = tuple(self._aggregate_host_runs(runs))
        host_overhead = tuple(self._calculate_overhead(host_aggregates))
        failures = tuple(self._failure_rows(source_runs))
        return ReportDocument(
            db_path=request.db_path,
            generated_at=datetime.now(timezone.utc).isoformat(),
            source_runs=tuple(source_runs),
            runs=runs,
            aggregates=aggregates,
            overhead=overhead,
            host_aggregates=host_aggregates,
            host_overhead=host_overhead,
            failures=failures,
        )

    def _build_run_row(
        self,
        source_run: ReportSourceRun,
        include_artifact_fallback: bool,
        artifact_root: Path,
        host_metrics_cache: HostMetricsCache,
    ) -> ReportRunRow:
        metrics = self._summary_workload_metrics(source_run.summary_metrics)
        if include_artifact_fallback and self._should_read_artifact_fallback(metrics):
            fallback_metrics = self._artifact_metrics(source_run, artifact_root)
            for key, value in fallback_metrics.items():
                metrics.setdefault(key, value)
        host_metrics, host_samples = load_host_metrics(
            source_run,
            artifact_root,
            host_metrics_cache,
        )
        return ReportRunRow(
            run_id=source_run.run_id,
            benchmark_name=source_run.benchmark_name,
            workload_name=source_run.workload_name,
            workload_tool=source_run.workload_tool,
            target_memory_gb=source_run.target_memory_gb,
            audit_mode=source_run.audit_mode,
            virtual_users=source_run.virtual_users,
            repetition=source_run.repetition,
            status=source_run.status,
            phase=source_run.phase,
            output_dir=source_run.output_dir,
            created_at=source_run.created_at,
            updated_at=source_run.updated_at,
            summary_notes=source_run.summary_notes,
            workload_metrics=metrics,
            host_metrics=host_metrics,
            host_samples=host_samples,
            artifacts=source_run.artifacts,
        )

    def _summary_workload_metrics(self, summary_metrics: dict[str, Any]) -> dict[str, Any]:
        workload = summary_metrics.get(WORKLOAD_SUMMARY_KEY)
        if isinstance(workload, dict):
            return _normalized_scalar_metrics(workload)
        return _normalized_scalar_metrics(summary_metrics)

    def _should_read_artifact_fallback(self, metrics: dict[str, Any]) -> bool:
        return any(metric_key not in metrics for metric_key in THROUGHPUT_METRIC_KEYS)

    def _artifact_metrics(self, source_run: ReportSourceRun, artifact_root: Path) -> dict[str, Any]:
        metrics: dict[str, Any] = {}
        for path in self._artifact_fallback_paths(source_run, artifact_root):
            if not path.exists() or not path.is_file():
                continue
            for key, value in _parse_key_value_metrics(path).items():
                metrics.setdefault(key, value)
        return metrics

    def _artifact_fallback_paths(
        self,
        source_run: ReportSourceRun,
        artifact_root: Path,
    ) -> Iterable[Path]:
        seen: set[Path] = set()
        for artifact in source_run.artifacts:
            artifact_path = _resolve_artifact_path(
                artifact.path,
                source_run.output_dir,
                artifact_root,
            )
            if artifact_path is None:
                continue
            lower_name = artifact_path.name.lower()
            artifact_type = artifact.artifact_type.lower()
            if (
                lower_name in ARTIFACT_FALLBACK_FILENAMES
                or "workload" in artifact_type
                or "hammerdb" in artifact_type
                or "stdout" in lower_name
            ):
                if artifact_path not in seen:
                    seen.add(artifact_path)
                    yield artifact_path
        for filename in ARTIFACT_FALLBACK_FILENAMES:
            output_path = _trusted_path(source_run.output_dir / filename, artifact_root)
            if output_path is None:
                continue
            if output_path not in seen:
                seen.add(output_path)
                yield output_path

    def _aggregate_runs(self, runs: Iterable[ReportRunRow]) -> list[AggregateRow]:
        grouped: dict[tuple[str, int], list[ReportRunRow]] = defaultdict(list)
        for run in runs:
            if run.status != SUCCESS_STATUS:
                continue
            grouped[(run.audit_mode, run.virtual_users)].append(run)
        return self._aggregate_grouped_runs(grouped, NON_AGGREGATE_METRIC_KEYS, "workload")

    def _aggregate_host_runs(self, runs: Iterable[ReportRunRow]) -> list[AggregateRow]:
        grouped: dict[tuple[str, int], list[ReportRunRow]] = defaultdict(list)
        for run in runs:
            if run.status != SUCCESS_STATUS or not run.host_metrics:
                continue
            grouped[(run.audit_mode, run.virtual_users)].append(run)
        return self._aggregate_grouped_runs(grouped, HOST_NON_AGGREGATE_METRIC_KEYS, "host")

    def _aggregate_grouped_runs(
        self,
        grouped: dict[tuple[str, int], list[ReportRunRow]],
        excluded_metric_keys: tuple[str, ...],
        metric_source: str,
    ) -> list[AggregateRow]:
        aggregates: list[AggregateRow] = []
        for (audit_mode, virtual_users), group_runs in sorted(grouped.items(), key=_group_sort_key):
            metrics_by_name: dict[str, list[float]] = defaultdict(list)
            for run in group_runs:
                metric_values = run.host_metrics if metric_source == "host" else run.workload_metrics
                for key, value in metric_values.items():
                    if key in excluded_metric_keys or not _is_number(value):
                        continue
                    metrics_by_name[key].append(float(value))
            aggregates.append(
                AggregateRow(
                    audit_mode=audit_mode,
                    virtual_users=virtual_users,
                    run_count=len(group_runs),
                    metrics={
                        key: MetricStats(
                            count=len(values),
                            mean=float(mean(values)),
                            minimum=min(values),
                            maximum=max(values),
                        )
                        for key, values in sorted(metrics_by_name.items())
                    },
                )
            )
        return aggregates

    def _calculate_overhead(self, aggregates: Iterable[AggregateRow]) -> list[OverheadRow]:
        by_group = {
            (aggregate.audit_mode, aggregate.virtual_users): aggregate
            for aggregate in aggregates
        }
        virtual_users = sorted({aggregate.virtual_users for aggregate in aggregates})
        overhead_rows: list[OverheadRow] = []
        for vu in virtual_users:
            audit_off = by_group.get((AUDIT_OFF_MODE, vu))
            audit_on = by_group.get((AUDIT_ON_MODE, vu))
            if audit_off is None or audit_on is None:
                continue
            metric_names = sorted(set(audit_off.metrics).intersection(audit_on.metrics))
            for metric_name in metric_names:
                off_mean = audit_off.metrics[metric_name].mean
                on_mean = audit_on.metrics[metric_name].mean
                delta = on_mean - off_mean
                percent_change = None if off_mean == 0 else (delta / off_mean) * 100.0
                overhead_rows.append(
                    OverheadRow(
                        virtual_users=vu,
                        metric_name=metric_name,
                        audit_off_mean=off_mean,
                        audit_on_mean=on_mean,
                        delta=delta,
                        percent_change=percent_change,
                    )
                )
        return overhead_rows

    def _failure_rows(self, source_runs: Iterable[ReportSourceRun]) -> list[FailureRow]:
        failures: list[FailureRow] = []
        for source_run in source_runs:
            if source_run.errors:
                for error in source_run.errors:
                    failures.append(
                        FailureRow(
                            run_id=source_run.run_id,
                            audit_mode=source_run.audit_mode,
                            virtual_users=source_run.virtual_users,
                            repetition=source_run.repetition,
                            phase=error.phase,
                            status=source_run.status,
                            exception_type=error.exception_type,
                            message=error.message,
                        )
                    )
                continue
            if source_run.status != SUCCESS_STATUS:
                failures.append(
                    FailureRow(
                        run_id=source_run.run_id,
                        audit_mode=source_run.audit_mode,
                        virtual_users=source_run.virtual_users,
                        repetition=source_run.repetition,
                        phase=source_run.phase,
                        status=source_run.status,
                        exception_type="",
                        message="",
                    )
                )
        return failures


def _normalized_scalar_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in metrics.items():
        if isinstance(value, dict) or isinstance(value, list) or isinstance(value, tuple):
            continue
        normalized_key = _normalize_metric_key(str(key))
        if not normalized_key:
            continue
        normalized[normalized_key] = _coerce_metric_value(value)
    return normalized


def _parse_key_value_metrics(path: Path) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        normalized_key = _normalize_metric_key(key)
        if not normalized_key:
            continue
        metrics[normalized_key] = _coerce_metric_value(value.strip())
    return metrics


def _normalize_metric_key(key: str) -> str:
    normalized = re.sub(r"[^a-z0-9_]+", "_", key.strip().lower())
    return normalized.strip("_")


def _coerce_metric_value(value: Any) -> Any:
    if isinstance(value, bool):
        return value
    if isinstance(value, int) or isinstance(value, float):
        return value
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return ""
    numeric_text = text.replace(",", "")
    try:
        return int(numeric_text)
    except ValueError:
        try:
            return float(numeric_text)
        except ValueError:
            return text


def _resolve_artifact_path(path: Path, output_dir: Path, artifact_root: Path) -> Path | None:
    candidate = path if path.is_absolute() or path.exists() else output_dir / path
    return _trusted_path(candidate, artifact_root)


def _trusted_path(path: Path, artifact_root: Path) -> Path | None:
    try:
        resolved_path = path.resolve(strict=False)
        resolved_root = artifact_root.resolve(strict=False)
    except OSError:
        return None
    if resolved_path == resolved_root or resolved_root in resolved_path.parents:
        return path
    return None


def _is_number(value: Any) -> bool:
    return not isinstance(value, bool) and isinstance(value, (int, float))


def _group_sort_key(item: tuple[tuple[str, int], list[ReportRunRow]]) -> tuple[int, str]:
    audit_mode, virtual_users = item[0]
    return (int(virtual_users), str(audit_mode))
