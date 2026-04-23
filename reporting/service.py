from __future__ import annotations

import csv
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from statistics import mean
from typing import Any, Iterable

from reporting.constants import (
    AGGREGATES_CSV_FILENAME,
    ARTIFACT_FALLBACK_FILENAMES,
    AUDIT_OFF_MODE,
    AUDIT_ON_MODE,
    DEFAULT_METRIC_KEYS,
    FAILURES_CSV_FILENAME,
    MISSING_VALUE,
    NON_AGGREGATE_METRIC_KEYS,
    OVERHEAD_CSV_FILENAME,
    REPORT_TITLE,
    RUNS_CSV_FILENAME,
    SECTION_ARTIFACTS,
    SECTION_FAILURES,
    SECTION_MATRIX,
    SECTION_OVERVIEW,
    SECTION_RUNS,
    SECTION_THROUGHPUT,
    SUCCESS_STATUS,
    THROUGHPUT_METRIC_KEYS,
    WORKLOAD_SUMMARY_KEY,
)
from reporting.dto import ReportGenerationRequest, ReportGenerationResult
from reporting.models import (
    AggregateRow,
    FailureRow,
    MetricStats,
    OverheadRow,
    ReportDocument,
    ReportRunRow,
    ReportSourceRun,
)
from reporting.repository import ReportingRepository


class BenchmarkReportService:
    def generate(self, request: ReportGenerationRequest) -> ReportGenerationResult:
        with ReportingRepository(request.db_path) as repository:
            source_runs = repository.list_runs()

        runs = tuple(
            self._build_run_row(source_run, request.include_artifact_fallback)
            for source_run in source_runs
        )
        aggregates = tuple(self._aggregate_runs(runs))
        overhead = tuple(self._calculate_overhead(aggregates))
        failures = tuple(self._failure_rows(source_runs))
        document = ReportDocument(
            db_path=request.db_path,
            generated_at=datetime.now(timezone.utc).isoformat(),
            source_runs=tuple(source_runs),
            runs=runs,
            aggregates=aggregates,
            overhead=overhead,
            failures=failures,
        )

        request.markdown_path.parent.mkdir(parents=True, exist_ok=True)
        csv_dir = request.resolved_csv_dir
        csv_dir.mkdir(parents=True, exist_ok=True)
        csv_paths = self._write_csvs(document, csv_dir)
        request.markdown_path.write_text(
            self.render_markdown(document, csv_paths),
            encoding="utf-8",
        )

        successful_runs = sum(1 for run in runs if run.status == SUCCESS_STATUS)
        return ReportGenerationResult(
            markdown_path=request.markdown_path,
            csv_paths=csv_paths,
            total_runs=len(runs),
            successful_runs=successful_runs,
            non_successful_runs=len(runs) - successful_runs,
        )

    def render_markdown(
        self,
        document: ReportDocument,
        csv_paths: dict[str, Path] | None = None,
    ) -> str:
        csv_paths = csv_paths or {}
        lines: list[str] = [
            f"# {REPORT_TITLE}",
            "",
            f"Generated: `{document.generated_at}`",
            "",
        ]
        lines.extend(self._overview_markdown(document))
        lines.extend(self._matrix_markdown(document.runs))
        lines.extend(self._throughput_markdown(document.overhead))
        lines.extend(self._runs_markdown(document.runs))
        lines.extend(self._failures_markdown(document.failures))
        lines.extend(self._artifacts_markdown(document, csv_paths))
        return "\n".join(lines).rstrip() + "\n"

    def _build_run_row(
        self,
        source_run: ReportSourceRun,
        include_artifact_fallback: bool,
    ) -> ReportRunRow:
        metrics = self._summary_workload_metrics(source_run.summary_metrics)
        if include_artifact_fallback and self._should_read_artifact_fallback(metrics):
            fallback_metrics = self._artifact_metrics(source_run)
            for key, value in fallback_metrics.items():
                metrics.setdefault(key, value)
        return ReportRunRow(
            run_id=source_run.run_id,
            benchmark_name=source_run.benchmark_name,
            workload_name=source_run.workload_name,
            workload_tool=source_run.workload_tool,
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
            artifacts=source_run.artifacts,
        )

    def _summary_workload_metrics(self, summary_metrics: dict[str, Any]) -> dict[str, Any]:
        workload = summary_metrics.get(WORKLOAD_SUMMARY_KEY)
        if isinstance(workload, dict):
            return _normalized_scalar_metrics(workload)
        return _normalized_scalar_metrics(summary_metrics)

    def _should_read_artifact_fallback(self, metrics: dict[str, Any]) -> bool:
        return any(metric_key not in metrics for metric_key in THROUGHPUT_METRIC_KEYS)

    def _artifact_metrics(self, source_run: ReportSourceRun) -> dict[str, Any]:
        metrics: dict[str, Any] = {}
        for path in self._artifact_fallback_paths(source_run):
            if not path.exists() or not path.is_file():
                continue
            for key, value in _parse_key_value_metrics(path).items():
                metrics.setdefault(key, value)
        return metrics

    def _artifact_fallback_paths(self, source_run: ReportSourceRun) -> Iterable[Path]:
        seen: set[Path] = set()
        for artifact in source_run.artifacts:
            artifact_path = _resolve_artifact_path(artifact.path, source_run.output_dir)
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
            output_path = source_run.output_dir / filename
            if output_path not in seen:
                seen.add(output_path)
                yield output_path

    def _aggregate_runs(self, runs: Iterable[ReportRunRow]) -> list[AggregateRow]:
        grouped: dict[tuple[str, int], list[ReportRunRow]] = defaultdict(list)
        for run in runs:
            if run.status != SUCCESS_STATUS:
                continue
            grouped[(run.audit_mode, run.virtual_users)].append(run)

        aggregates: list[AggregateRow] = []
        for (audit_mode, virtual_users), group_runs in sorted(grouped.items(), key=_group_sort_key):
            metrics_by_name: dict[str, list[float]] = defaultdict(list)
            for run in group_runs:
                for key, value in run.workload_metrics.items():
                    if key in NON_AGGREGATE_METRIC_KEYS or not _is_number(value):
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

    def _write_csvs(self, document: ReportDocument, csv_dir: Path) -> dict[str, Path]:
        paths = {
            "runs": csv_dir / RUNS_CSV_FILENAME,
            "aggregates": csv_dir / AGGREGATES_CSV_FILENAME,
            "overhead": csv_dir / OVERHEAD_CSV_FILENAME,
            "failures": csv_dir / FAILURES_CSV_FILENAME,
        }
        self._write_runs_csv(document.runs, paths["runs"])
        self._write_aggregates_csv(document.aggregates, paths["aggregates"])
        self._write_overhead_csv(document.overhead, paths["overhead"])
        self._write_failures_csv(document.failures, paths["failures"])
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

    def _write_aggregates_csv(self, aggregates: tuple[AggregateRow, ...], path: Path) -> None:
        metric_keys = _ordered_metric_keys(
            metric_key for aggregate in aggregates for metric_key in aggregate.metrics
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

    def _write_overhead_csv(self, overhead: tuple[OverheadRow, ...], path: Path) -> None:
        metric_keys = _ordered_metric_keys(row.metric_name for row in overhead)
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

    def _overview_markdown(self, document: ReportDocument) -> list[str]:
        successful_runs = sum(1 for run in document.runs if run.status == SUCCESS_STATUS)
        non_successful_runs = len(document.runs) - successful_runs
        benchmark_names = _join_values(run.benchmark_name for run in document.runs)
        return [
            f"## {SECTION_OVERVIEW}",
            "",
            "| Field | Value |",
            "| --- | --- |",
            f"| Database | `{_md_escape(str(document.db_path))}` |",
            f"| Benchmarks | {_md_escape(benchmark_names)} |",
            f"| Total runs | {len(document.runs)} |",
            f"| Successful runs | {successful_runs} |",
            f"| Failed/skipped/incomplete runs | {non_successful_runs} |",
            "",
            "Failed, skipped, and incomplete runs are listed in diagnostics and excluded from aggregates.",
            "",
        ]

    def _matrix_markdown(self, runs: tuple[ReportRunRow, ...]) -> list[str]:
        modes = _join_values(run.audit_mode for run in runs)
        vu_ladder = _join_values(str(run.virtual_users) for run in runs)
        repetitions = _join_values(str(run.repetition) for run in runs)
        matrix: dict[tuple[str, int], list[ReportRunRow]] = defaultdict(list)
        for run in runs:
            matrix[(run.audit_mode, run.virtual_users)].append(run)

        lines = [
            f"## {SECTION_MATRIX}",
            "",
            f"- Modes observed: {modes}",
            f"- Virtual users observed: {vu_ladder}",
            f"- Repetitions observed: {repetitions}",
            "",
            "| Audit mode | Virtual users | Repetitions | Successful | Non-successful |",
            "| --- | ---: | --- | ---: | ---: |",
        ]
        for (audit_mode, virtual_users), group_runs in sorted(matrix.items(), key=_group_sort_key):
            successful = sum(1 for run in group_runs if run.status == SUCCESS_STATUS)
            reps = _join_values(str(run.repetition) for run in group_runs)
            lines.append(
                "| "
                f"{_md_escape(audit_mode)} | "
                f"{virtual_users} | "
                f"{_md_escape(reps)} | "
                f"{successful} | "
                f"{len(group_runs) - successful} |"
            )
        lines.append("")
        return lines

    def _throughput_markdown(self, overhead: tuple[OverheadRow, ...]) -> list[str]:
        throughput_rows = [
            row for row in overhead if row.metric_name in THROUGHPUT_METRIC_KEYS
        ]
        lines = [
            f"## {SECTION_THROUGHPUT}",
            "",
        ]
        if not throughput_rows:
            lines.extend(
                [
                    f"Throughput comparison is {MISSING_VALUE} because TPM/NOPM metrics were not available for matching audit modes.",
                    "",
                ]
            )
            return lines
        lines.extend(
            [
                "| Virtual users | Metric | audit_off mean | audit_on mean | Delta | Percent change |",
                "| ---: | --- | ---: | ---: | ---: | ---: |",
            ]
        )
        for row in sorted(throughput_rows, key=lambda item: (item.virtual_users, item.metric_name)):
            lines.append(
                "| "
                f"{row.virtual_users} | "
                f"{_md_escape(row.metric_name)} | "
                f"{_format_number(row.audit_off_mean)} | "
                f"{_format_number(row.audit_on_mean)} | "
                f"{_format_number(row.delta)} | "
                f"{_format_percent(row.percent_change)} |"
            )
        lines.extend(
            [
                "",
                "For TPM/NOPM, a negative percent change means audit_on throughput was lower than audit_off.",
                "",
            ]
        )
        return lines

    def _runs_markdown(self, runs: tuple[ReportRunRow, ...]) -> list[str]:
        lines = [
            f"## {SECTION_RUNS}",
            "",
            "| Run | Audit mode | VUs | Rep | Status | Phase | Output dir | TPM | NOPM | Latency ms | Duration s | Benchmark status |",
            "| ---: | --- | ---: | ---: | --- | --- | --- | ---: | ---: | ---: | ---: | --- |",
        ]
        for run in runs:
            lines.append(
                "| "
                f"{run.run_id} | "
                f"{_md_escape(run.audit_mode)} | "
                f"{run.virtual_users} | "
                f"{run.repetition} | "
                f"{_md_escape(run.status)} | "
                f"{_md_escape(run.phase)} | "
                f"`{_md_escape(str(run.output_dir))}` | "
                f"{_format_metric(run.workload_metrics.get('tpm'))} | "
                f"{_format_metric(run.workload_metrics.get('nopm'))} | "
                f"{_format_metric(run.workload_metrics.get('latency_ms'))} | "
                f"{_format_metric(run.workload_metrics.get('duration_seconds'))} | "
                f"{_md_escape(_display_value(run.workload_metrics.get('benchmark_status')))} |"
            )
        lines.append("")
        return lines

    def _failures_markdown(self, failures: tuple[FailureRow, ...]) -> list[str]:
        lines = [
            f"## {SECTION_FAILURES}",
            "",
        ]
        if not failures:
            lines.extend(["No failed, skipped, or incomplete runs were found.", ""])
            return lines
        lines.extend(
            [
                "| Run | Audit mode | VUs | Rep | Phase | Status | Exception | Message |",
                "| ---: | --- | ---: | ---: | --- | --- | --- | --- |",
            ]
        )
        for failure in failures:
            lines.append(
                "| "
                f"{failure.run_id} | "
                f"{_md_escape(failure.audit_mode)} | "
                f"{failure.virtual_users} | "
                f"{failure.repetition} | "
                f"{_md_escape(failure.phase)} | "
                f"{_md_escape(failure.status)} | "
                f"{_md_escape(_display_value(failure.exception_type))} | "
                f"{_md_escape(_display_value(failure.message))} |"
            )
        lines.append("")
        return lines

    def _artifacts_markdown(
        self,
        document: ReportDocument,
        csv_paths: dict[str, Path],
    ) -> list[str]:
        artifact_count = sum(len(run.artifacts) for run in document.source_runs)
        runs_with_artifacts = sum(1 for run in document.source_runs if run.artifacts)
        lines = [
            f"## {SECTION_ARTIFACTS}",
            "",
            f"- Registered artifacts: {artifact_count} across {runs_with_artifacts} runs.",
            "- Raw output directories are shown in the per-run table.",
        ]
        if csv_paths:
            lines.extend(["", "| CSV | Path |", "| --- | --- |"])
            for name in ("runs", "aggregates", "overhead", "failures"):
                path = csv_paths.get(name)
                if path is not None:
                    lines.append(f"| {_md_escape(name)} | `{_md_escape(str(path))}` |")
        lines.append("")
        return lines


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


def _resolve_artifact_path(path: Path, output_dir: Path) -> Path:
    if path.is_absolute() or path.exists():
        return path
    return output_dir / path


def _is_number(value: Any) -> bool:
    return not isinstance(value, bool) and isinstance(value, (int, float))


def _ordered_metric_keys(metric_keys: Iterable[str]) -> list[str]:
    seen = {key for key in metric_keys if key}
    ordered = [key for key in DEFAULT_METRIC_KEYS if key in seen]
    ordered.extend(sorted(seen.difference(DEFAULT_METRIC_KEYS)))
    return ordered


def _group_sort_key(item: tuple[str, int] | tuple[tuple[str, int], list[ReportRunRow]]) -> tuple[int, str]:
    if len(item) == 2 and isinstance(item[0], tuple):
        audit_mode, virtual_users = item[0]
    else:
        audit_mode, virtual_users = item  # type: ignore[misc]
    return (int(virtual_users), str(audit_mode))


def _join_values(values: Iterable[str]) -> str:
    unique_values = sorted({value for value in values if value}, key=_natural_sort_key)
    return ", ".join(unique_values) if unique_values else MISSING_VALUE


def _natural_sort_key(value: str) -> tuple[int, str]:
    try:
        return (0, f"{int(value):010d}")
    except ValueError:
        return (1, value)


def _csv_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.10g}"
    return str(value)


def _display_value(value: Any) -> str:
    if value is None or value == "":
        return MISSING_VALUE
    return str(value)


def _format_metric(value: Any) -> str:
    if _is_number(value):
        return _format_number(float(value))
    return _md_escape(_display_value(value))


def _format_number(value: float) -> str:
    if abs(value) >= 100:
        text = f"{value:.2f}"
    elif abs(value) >= 1:
        text = f"{value:.3f}"
    else:
        text = f"{value:.4f}"
    return text.rstrip("0").rstrip(".")


def _format_percent(value: float | None) -> str:
    if value is None:
        return MISSING_VALUE
    return f"{_format_number(value)}%"


def _md_escape(value: str) -> str:
    return value.replace("\n", " ").replace("|", "\\|")
