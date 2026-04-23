from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

from reporting.constants import (
    MISSING_VALUE,
    REPORT_TITLE,
    SECTION_ARTIFACTS,
    SECTION_FAILURES,
    SECTION_HOST_METRICS,
    SECTION_MATRIX,
    SECTION_OVERVIEW,
    SECTION_RUNS,
    SECTION_THROUGHPUT,
    SUCCESS_STATUS,
    THROUGHPUT_METRIC_KEYS,
)
from reporting.models import OverheadRow, ReportDocument, ReportRunRow


class MarkdownReportRenderer:
    def render(
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
        lines.extend(self._host_metrics_markdown(document))
        lines.extend(self._runs_markdown(document.runs))
        lines.extend(self._failures_markdown(document))
        lines.extend(self._artifacts_markdown(document, csv_paths))
        return "\n".join(lines).rstrip() + "\n"

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
        lines = [f"## {SECTION_THROUGHPUT}", ""]
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

    def _host_metrics_markdown(self, document: ReportDocument) -> list[str]:
        lines = [f"## {SECTION_HOST_METRICS}", ""]
        if not any(run.host_metrics for run in document.runs):
            lines.extend(
                [
                    f"CPU and memory metrics are {MISSING_VALUE} because no host_metrics_csv artifacts were found.",
                    "",
                ]
            )
            return lines

        comparison_rows = [
            row
            for row in document.host_overhead
            if row.metric_name in _host_comparison_metric_keys()
        ]
        if comparison_rows:
            lines.extend(
                [
                    "| Virtual users | Metric | audit_off mean | audit_on mean | Delta | Percent change |",
                    "| ---: | --- | ---: | ---: | ---: | ---: |",
                ]
            )
            for row in sorted(
                comparison_rows,
                key=lambda item: (item.virtual_users, _host_metric_order(item.metric_name)),
            ):
                lines.append(
                    "| "
                    f"{row.virtual_users} | "
                    f"{_md_escape(_host_metric_label(row.metric_name))} | "
                    f"{_format_number(row.audit_off_mean)} | "
                    f"{_format_number(row.audit_on_mean)} | "
                    f"{_format_number(row.delta)} | "
                    f"{_format_percent(row.percent_change)} |"
                )
            lines.append("")
        else:
            lines.extend(
                [
                    f"Per-VU CPU/memory comparison is {MISSING_VALUE} because matching audit modes were not available.",
                    "",
                ]
            )

        lines.extend(
            [
                "| Run | Audit mode | VUs | Rep | Samples | Total CPU avg | Total CPU max | SQL CPU avg | Memory used avg % | Memory used max % | SQL working set avg MB |",
                "| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
            ]
        )
        for run in document.runs:
            metrics = run.host_metrics
            lines.append(
                "| "
                f"{run.run_id} | "
                f"{_md_escape(run.audit_mode)} | "
                f"{run.virtual_users} | "
                f"{run.repetition} | "
                f"{_format_metric(metrics.get('sample_count'))} | "
                f"{_format_metric(metrics.get('total_cpu_percent_avg'))} | "
                f"{_format_metric(metrics.get('total_cpu_percent_max'))} | "
                f"{_format_metric(metrics.get('sql_cpu_percent_avg'))} | "
                f"{_format_metric(metrics.get('memory_used_percent_avg'))} | "
                f"{_format_metric(metrics.get('memory_used_percent_max'))} | "
                f"{_format_metric(metrics.get('sql_working_set_mb_avg'))} |"
            )
        lines.append("")
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

    def _failures_markdown(self, document: ReportDocument) -> list[str]:
        lines = [f"## {SECTION_FAILURES}", ""]
        if not document.failures:
            lines.extend(["No failed, skipped, or incomplete runs were found.", ""])
            return lines
        lines.extend(
            [
                "| Run | Audit mode | VUs | Rep | Phase | Status | Exception | Message |",
                "| ---: | --- | ---: | ---: | --- | --- | --- | --- |",
            ]
        )
        for failure in document.failures:
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
            for name in (
                "runs",
                "aggregates",
                "overhead",
                "failures",
                "host_runs",
                "host_aggregates",
                "host_overhead",
                "host_samples",
            ):
                path = csv_paths.get(name)
                if path is not None:
                    lines.append(f"| {_md_escape(name)} | `{_md_escape(str(path))}` |")
        lines.append("")
        return lines


def _host_comparison_metric_keys() -> tuple[str, ...]:
    return (
        "total_cpu_percent_avg",
        "total_cpu_percent_max",
        "sql_cpu_percent_avg",
        "sql_cpu_percent_max",
        "memory_used_percent_avg",
        "memory_used_percent_max",
        "sql_working_set_mb_avg",
    )


def _host_metric_order(metric_name: str) -> int:
    try:
        return _host_comparison_metric_keys().index(metric_name)
    except ValueError:
        return len(_host_comparison_metric_keys())


def _host_metric_label(metric_name: str) -> str:
    labels = {
        "total_cpu_percent_avg": "Total CPU avg %",
        "total_cpu_percent_max": "Total CPU max %",
        "sql_cpu_percent_avg": "SQL CPU avg %",
        "sql_cpu_percent_max": "SQL CPU max %",
        "memory_used_percent_avg": "Memory used avg %",
        "memory_used_percent_max": "Memory used max %",
        "sql_working_set_mb_avg": "SQL working set avg MB",
    }
    return labels.get(metric_name, metric_name)


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


def _display_value(value: Any) -> str:
    if value is None or value == "":
        return MISSING_VALUE
    return str(value)


def _is_number(value: Any) -> bool:
    return not isinstance(value, bool) and isinstance(value, (int, float))


def _group_sort_key(item: tuple[tuple[str, int], list[ReportRunRow]]) -> tuple[int, str]:
    audit_mode, virtual_users = item[0]
    return (int(virtual_users), str(audit_mode))


def _join_values(values: Iterable[str]) -> str:
    unique_values = sorted({value for value in values if value}, key=_natural_sort_key)
    return ", ".join(unique_values) if unique_values else MISSING_VALUE


def _natural_sort_key(value: str) -> tuple[int, str]:
    try:
        return (0, f"{int(value):010d}")
    except ValueError:
        return (1, value)


def _md_escape(value: str) -> str:
    return value.replace("\n", " ").replace("|", "\\|")
