from __future__ import annotations

import html as html_lib
from pathlib import Path

from reporting.constants import (
    AUDIT_OFF_MODE,
    AUDIT_ON_MODE,
    MISSING_VALUE,
    REPORT_TITLE,
    SECTION_ARTIFACTS,
    SECTION_FAILURES,
    SECTION_HOST_METRICS,
    SECTION_OVERVIEW,
    SECTION_THROUGHPUT,
    SUCCESS_STATUS,
    THROUGHPUT_METRIC_KEYS,
)
from reporting.models import AggregateRow, HostMetricSample, OverheadRow, ReportDocument, ReportRunRow


class HtmlReportRenderer:
    def render(
        self,
        document: ReportDocument,
        csv_paths: dict[str, Path] | None = None,
    ) -> str:
        csv_paths = csv_paths or {}
        body = [
            "<!doctype html>",
            '<html lang="en">',
            "<head>",
            '<meta charset="utf-8">',
            '<meta name="viewport" content="width=device-width, initial-scale=1">',
            f"<title>{_html(REPORT_TITLE)}</title>",
            "<style>",
            _html_styles(),
            "</style>",
            "</head>",
            "<body>",
            f"<h1>{_html(REPORT_TITLE)}</h1>",
            f'<p class="meta">Generated: <code>{_html(document.generated_at)}</code></p>',
            self._overview(document),
            self._throughput(document),
            self._host_metrics(document),
            self._failures(document),
            self._csv_links(csv_paths),
            "</body>",
            "</html>",
        ]
        return "\n".join(body)

    def _overview(self, document: ReportDocument) -> str:
        successful_runs = sum(1 for run in document.runs if run.status == SUCCESS_STATUS)
        non_successful_runs = len(document.runs) - successful_runs
        benchmark_names = _join_values(run.benchmark_name for run in document.runs)
        return "\n".join(
            [
                f"<section><h2>{_html(SECTION_OVERVIEW)}</h2>",
                '<table class="compact"><tbody>',
                f"<tr><th>Database</th><td><code>{_html(str(document.db_path))}</code></td></tr>",
                f"<tr><th>Benchmarks</th><td>{_html(benchmark_names)}</td></tr>",
                f"<tr><th>Total runs</th><td>{len(document.runs)}</td></tr>",
                f"<tr><th>Successful runs</th><td>{successful_runs}</td></tr>",
                f"<tr><th>Failed/skipped/incomplete runs</th><td>{non_successful_runs}</td></tr>",
                "</tbody></table>",
                "</section>",
            ]
        )

    def _throughput(self, document: ReportDocument) -> str:
        rows = [
            row for row in document.overhead if row.metric_name in THROUGHPUT_METRIC_KEYS
        ]
        if not rows:
            return (
                f"<section><h2>{_html(SECTION_THROUGHPUT)}</h2>"
                f"<p>Throughput comparison is {_html(MISSING_VALUE)}.</p></section>"
            )
        table_rows = [
            "<tr><th>Virtual users</th><th>Metric</th><th>audit_off mean</th>"
            "<th>audit_on mean</th><th>Delta</th><th>Percent change</th></tr>"
        ]
        for row in sorted(rows, key=lambda item: (item.virtual_users, item.metric_name)):
            table_rows.append(
                "<tr>"
                f"<td>{row.virtual_users}</td>"
                f"<td>{_html(row.metric_name)}</td>"
                f"<td>{_format_number(row.audit_off_mean)}</td>"
                f"<td>{_format_number(row.audit_on_mean)}</td>"
                f"<td>{_format_number(row.delta)}</td>"
                f"<td>{_html(_format_percent(row.percent_change))}</td>"
                "</tr>"
            )
        return "\n".join(
            [
                f"<section><h2>{_html(SECTION_THROUGHPUT)}</h2>",
                '<table class="numeric">',
                *table_rows,
                "</table>",
                "</section>",
            ]
        )

    def _host_metrics(self, document: ReportDocument) -> str:
        if not any(run.host_metrics for run in document.runs):
            return (
                f"<section><h2>{_html(SECTION_HOST_METRICS)}</h2>"
                f"<p>CPU and memory metrics are {_html(MISSING_VALUE)} because no host metrics CSV artifacts were found.</p>"
                "</section>"
            )

        comparison_rows = [
            row
            for row in document.host_overhead
            if row.metric_name in _host_comparison_metric_keys()
        ]
        charts = [
            _bar_chart(
                title=_host_metric_label(metric_name),
                rows=_bar_chart_rows(document.host_aggregates, metric_name),
                value_suffix=_metric_suffix(metric_name),
            )
            for metric_name in _host_chart_metric_keys()
        ]
        return "\n".join(
            [
                f"<section><h2>{_html(SECTION_HOST_METRICS)}</h2>",
                self._host_comparison_table(comparison_rows),
                '<div class="charts">',
                *(chart for chart in charts if chart),
                "</div>",
                "<h3>Per-run summaries</h3>",
                self._host_run_table(document.runs),
                "<h3>Per-run time series</h3>",
                self._host_time_series(document.runs),
                "</section>",
            ]
        )

    def _host_comparison_table(self, rows: list[OverheadRow]) -> str:
        if not rows:
            return f"<p>Per-VU CPU/memory comparison is {_html(MISSING_VALUE)}.</p>"
        table_rows = [
            "<tr><th>Virtual users</th><th>Metric</th><th>audit_off mean</th>"
            "<th>audit_on mean</th><th>Delta</th><th>Percent change</th></tr>"
        ]
        for row in sorted(rows, key=lambda item: (item.virtual_users, _host_metric_order(item.metric_name))):
            table_rows.append(
                "<tr>"
                f"<td>{row.virtual_users}</td>"
                f"<td>{_html(_host_metric_label(row.metric_name))}</td>"
                f"<td>{_format_number(row.audit_off_mean)}</td>"
                f"<td>{_format_number(row.audit_on_mean)}</td>"
                f"<td>{_format_number(row.delta)}</td>"
                f"<td>{_html(_format_percent(row.percent_change))}</td>"
                "</tr>"
            )
        return "\n".join(['<table class="numeric">', *table_rows, "</table>"])

    def _host_run_table(self, runs: tuple[ReportRunRow, ...]) -> str:
        rows = [
            "<tr><th>Run</th><th>Audit mode</th><th>VUs</th><th>Rep</th><th>Samples</th>"
            "<th>Total CPU avg</th><th>Total CPU max</th><th>SQL CPU avg</th>"
            "<th>Memory used avg %</th><th>Memory used max %</th><th>SQL working set avg MB</th></tr>"
        ]
        for run in runs:
            metrics = run.host_metrics
            rows.append(
                "<tr>"
                f"<td>{run.run_id}</td>"
                f"<td>{_html(run.audit_mode)}</td>"
                f"<td>{run.virtual_users}</td>"
                f"<td>{run.repetition}</td>"
                f"<td>{_format_metric(metrics.get('sample_count'))}</td>"
                f"<td>{_format_metric(metrics.get('total_cpu_percent_avg'))}</td>"
                f"<td>{_format_metric(metrics.get('total_cpu_percent_max'))}</td>"
                f"<td>{_format_metric(metrics.get('sql_cpu_percent_avg'))}</td>"
                f"<td>{_format_metric(metrics.get('memory_used_percent_avg'))}</td>"
                f"<td>{_format_metric(metrics.get('memory_used_percent_max'))}</td>"
                f"<td>{_format_metric(metrics.get('sql_working_set_mb_avg'))}</td>"
                "</tr>"
            )
        return "\n".join(['<table class="numeric">', *rows, "</table>"])

    def _host_time_series(self, runs: tuple[ReportRunRow, ...]) -> str:
        charts: list[str] = []
        for run in runs:
            if not run.host_samples:
                continue
            charts.append(
                _line_chart(
                    title=f"Run {run.run_id} CPU ({run.audit_mode}, {run.virtual_users} VUs, rep {run.repetition})",
                    samples=run.host_samples,
                    series=(
                        ("total_cpu_percent", "Total CPU %", "#2563eb"),
                        ("sql_cpu_percent", "SQL CPU %", "#dc2626"),
                    ),
                    value_suffix="%",
                )
            )
            charts.append(
                _line_chart(
                    title=f"Run {run.run_id} memory ({run.audit_mode}, {run.virtual_users} VUs, rep {run.repetition})",
                    samples=run.host_samples,
                    series=(("memory_used_percent", "Memory used %", "#059669"),),
                    value_suffix="%",
                )
            )
        charts = [chart for chart in charts if chart]
        if not charts:
            return f"<p>Per-run time-series charts are {_html(MISSING_VALUE)}.</p>"
        return "\n".join(['<div class="charts">', *charts, "</div>"])

    def _failures(self, document: ReportDocument) -> str:
        if not document.failures:
            return f"<section><h2>{_html(SECTION_FAILURES)}</h2><p>No failed, skipped, or incomplete runs were found.</p></section>"
        rows = [
            "<tr><th>Run</th><th>Audit mode</th><th>VUs</th><th>Rep</th>"
            "<th>Phase</th><th>Status</th><th>Exception</th><th>Message</th></tr>"
        ]
        for failure in document.failures:
            rows.append(
                "<tr>"
                f"<td>{failure.run_id}</td>"
                f"<td>{_html(failure.audit_mode)}</td>"
                f"<td>{failure.virtual_users}</td>"
                f"<td>{failure.repetition}</td>"
                f"<td>{_html(failure.phase)}</td>"
                f"<td>{_html(failure.status)}</td>"
                f"<td>{_html(_display_value(failure.exception_type))}</td>"
                f"<td>{_html(_display_value(failure.message))}</td>"
                "</tr>"
            )
        return "\n".join(
            [
                f"<section><h2>{_html(SECTION_FAILURES)}</h2>",
                '<table class="compact">',
                *rows,
                "</table>",
                "</section>",
            ]
        )

    def _csv_links(self, csv_paths: dict[str, Path]) -> str:
        if not csv_paths:
            return ""
        rows = [
            f"<tr><td>{_html(name)}</td><td><code>{_html(str(path))}</code></td></tr>"
            for name, path in sorted(csv_paths.items())
        ]
        return "\n".join(
            [
                f"<section><h2>{_html(SECTION_ARTIFACTS)}</h2>",
                '<table class="compact"><tr><th>CSV</th><th>Path</th></tr>',
                *rows,
                "</table>",
                "</section>",
            ]
        )


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


def _host_chart_metric_keys() -> tuple[str, ...]:
    return (
        "total_cpu_percent_avg",
        "sql_cpu_percent_avg",
        "memory_used_percent_avg",
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


def _metric_suffix(metric_name: str) -> str:
    if metric_name.endswith("_percent_avg") or metric_name.endswith("_percent_max"):
        return "%"
    if metric_name.endswith("_mb_avg") or metric_name.endswith("_mb_max"):
        return " MB"
    return ""


def _bar_chart_rows(
    aggregates: tuple[AggregateRow, ...],
    metric_name: str,
) -> list[tuple[int, float, float]]:
    by_group = {
        (aggregate.audit_mode, aggregate.virtual_users): aggregate
        for aggregate in aggregates
    }
    rows: list[tuple[int, float, float]] = []
    for virtual_users in sorted({aggregate.virtual_users for aggregate in aggregates}):
        off = by_group.get((AUDIT_OFF_MODE, virtual_users))
        on = by_group.get((AUDIT_ON_MODE, virtual_users))
        if off is None or on is None:
            continue
        off_stats = off.metrics.get(metric_name)
        on_stats = on.metrics.get(metric_name)
        if off_stats is None or on_stats is None:
            continue
        rows.append((virtual_users, off_stats.mean, on_stats.mean))
    return rows


def _bar_chart(
    title: str,
    rows: list[tuple[int, float, float]],
    value_suffix: str,
) -> str:
    if not rows:
        return ""
    width = max(560, 120 + (len(rows) * 120))
    height = 300
    margin_left = 58
    margin_right = 24
    margin_top = 42
    margin_bottom = 54
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom
    bottom = margin_top + plot_height
    max_value = _nice_max(max(max(off, on) for _, off, on in rows))
    group_width = plot_width / len(rows)
    bar_width = min(28.0, group_width * 0.28)
    svg: list[str] = [
        '<figure class="chart">',
        f"<figcaption>{_html(title)}</figcaption>",
        f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="{_html(title)}">',
        f'<line class="axis" x1="{margin_left}" y1="{bottom}" x2="{width - margin_right}" y2="{bottom}" />',
        f'<line class="axis" x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" y2="{bottom}" />',
        f'<text class="tick" x="{margin_left - 8}" y="{margin_top + 4}" text-anchor="end">{_html(_format_number(max_value))}</text>',
        f'<text class="tick" x="{margin_left - 8}" y="{bottom}" text-anchor="end">0</text>',
        f'<text class="legend" x="{margin_left}" y="22" fill="#2563eb">audit_off</text>',
        f'<text class="legend" x="{margin_left + 86}" y="22" fill="#dc2626">audit_on</text>',
    ]
    for index, (virtual_users, audit_off, audit_on) in enumerate(rows):
        group_x = margin_left + (index * group_width) + (group_width / 2)
        off_height = (audit_off / max_value) * plot_height if max_value else 0
        on_height = (audit_on / max_value) * plot_height if max_value else 0
        off_x = group_x - bar_width - 3
        on_x = group_x + 3
        svg.extend(
            [
                f'<rect class="bar-off" x="{off_x:.2f}" y="{bottom - off_height:.2f}" width="{bar_width:.2f}" height="{off_height:.2f}" />',
                f'<rect class="bar-on" x="{on_x:.2f}" y="{bottom - on_height:.2f}" width="{bar_width:.2f}" height="{on_height:.2f}" />',
                f'<text class="tick" x="{group_x:.2f}" y="{bottom + 18}" text-anchor="middle">{virtual_users} VUs</text>',
                f'<text class="value" x="{off_x + bar_width / 2:.2f}" y="{bottom - off_height - 5:.2f}" text-anchor="middle">{_html(_format_number(audit_off))}{_html(value_suffix)}</text>',
                f'<text class="value" x="{on_x + bar_width / 2:.2f}" y="{bottom - on_height - 5:.2f}" text-anchor="middle">{_html(_format_number(audit_on))}{_html(value_suffix)}</text>',
            ]
        )
    svg.extend(["</svg>", "</figure>"])
    return "\n".join(svg)


def _line_chart(
    title: str,
    samples: tuple[HostMetricSample, ...],
    series: tuple[tuple[str, str, str], ...],
    value_suffix: str,
) -> str:
    series_values = [
        (
            field_name,
            label,
            color,
            [(sample.sample_index, _sample_value(sample, field_name)) for sample in samples],
        )
        for field_name, label, color in series
    ]
    series_values = [
        (field_name, label, color, [(index, value) for index, value in values if value is not None])
        for field_name, label, color, values in series_values
    ]
    series_values = [item for item in series_values if item[3]]
    if not series_values:
        return ""
    width = 640
    height = 300
    margin_left = 58
    margin_right = 24
    margin_top = 42
    margin_bottom = 50
    plot_width = width - margin_left - margin_right
    plot_height = height - margin_top - margin_bottom
    bottom = margin_top + plot_height
    all_values = [value for _, _, _, values in series_values for _, value in values]
    max_value = _nice_max(max(all_values))
    min_index = min(sample.sample_index for sample in samples)
    max_index = max(sample.sample_index for sample in samples)
    index_span = max(max_index - min_index, 1)
    svg: list[str] = [
        '<figure class="chart">',
        f"<figcaption>{_html(title)}</figcaption>",
        f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="{_html(title)}">',
        f'<line class="axis" x1="{margin_left}" y1="{bottom}" x2="{width - margin_right}" y2="{bottom}" />',
        f'<line class="axis" x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" y2="{bottom}" />',
        f'<text class="tick" x="{margin_left - 8}" y="{margin_top + 4}" text-anchor="end">{_html(_format_number(max_value))}{_html(value_suffix)}</text>',
        f'<text class="tick" x="{margin_left - 8}" y="{bottom}" text-anchor="end">0</text>',
    ]
    legend_x = margin_left
    for _, label, color, _ in series_values:
        svg.append(f'<text class="legend" x="{legend_x}" y="22" fill="{color}">{_html(label)}</text>')
        legend_x += 110
    for _, _, color, values in series_values:
        points: list[str] = []
        for sample_index, value in values:
            x = margin_left + ((sample_index - min_index) / index_span) * plot_width
            y = bottom - ((value / max_value) * plot_height if max_value else 0)
            points.append(f"{x:.2f},{y:.2f}")
        svg.append(
            f'<polyline class="series" points="{" ".join(points)}" stroke="{color}" />'
        )
        for point in points:
            x, y = point.split(",", 1)
            svg.append(f'<circle class="point" cx="{x}" cy="{y}" r="2.5" fill="{color}" />')
    svg.extend(["</svg>", "</figure>"])
    return "\n".join(svg)


def _sample_value(sample: HostMetricSample, field_name: str) -> float | None:
    value = getattr(sample, field_name)
    if _is_number(value):
        return float(value)
    return None


def _nice_max(value: float) -> float:
    if value <= 0:
        return 1.0
    if value <= 1:
        return 1.0
    magnitude = 10 ** (len(str(int(value))) - 1)
    return float(((int(value / magnitude) + 1) * magnitude))


def _format_metric(value: object) -> str:
    if _is_number(value):
        return _format_number(float(value))
    return _html(_display_value(value))


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


def _display_value(value: object) -> str:
    if value is None or value == "":
        return MISSING_VALUE
    return str(value)


def _join_values(values: object) -> str:
    unique_values = sorted({value for value in values if value}, key=_natural_sort_key)
    return ", ".join(unique_values) if unique_values else MISSING_VALUE


def _natural_sort_key(value: str) -> tuple[int, str]:
    try:
        return (0, f"{int(value):010d}")
    except ValueError:
        return (1, value)


def _is_number(value: object) -> bool:
    return not isinstance(value, bool) and isinstance(value, (int, float))


def _html(value: object) -> str:
    return html_lib.escape(str(value), quote=True)


def _html_styles() -> str:
    return """
body {
  color: #172033;
  font-family: Arial, Helvetica, sans-serif;
  line-height: 1.45;
  margin: 24px;
}
h1, h2, h3 {
  color: #0f172a;
}
section {
  border-top: 1px solid #d7dde8;
  margin-top: 28px;
  padding-top: 18px;
}
table {
  border-collapse: collapse;
  margin: 12px 0 20px;
  width: 100%;
}
th, td {
  border: 1px solid #d7dde8;
  padding: 6px 8px;
  text-align: left;
  vertical-align: top;
}
th {
  background: #f4f6fa;
}
table.numeric td {
  text-align: right;
}
table.numeric td:nth-child(2),
table.numeric th:nth-child(2) {
  text-align: left;
}
code {
  background: #f4f6fa;
  border-radius: 4px;
  padding: 1px 4px;
}
.meta {
  color: #526071;
}
.charts {
  display: grid;
  gap: 16px;
  grid-template-columns: repeat(auto-fit, minmax(520px, 1fr));
}
.chart {
  border: 1px solid #d7dde8;
  margin: 0;
  overflow-x: auto;
  padding: 12px;
}
.chart figcaption {
  font-weight: 700;
  margin-bottom: 8px;
}
.axis {
  stroke: #667085;
  stroke-width: 1;
}
.bar-off {
  fill: #2563eb;
}
.bar-on {
  fill: #dc2626;
}
.series {
  fill: none;
  stroke-linejoin: round;
  stroke-width: 2.2;
}
.tick, .value, .legend {
  font-size: 11px;
}
""".strip()
