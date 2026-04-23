# Reporting Context

The reporting package turns an existing Benchpress SQLite database and registered run artifacts into human-readable Markdown, HTML, and CSV exports. It is read-only with respect to `benchpress.sqlite3`; it does not create or migrate schema.

Main modules:

- `repository.py`: SQLite read queries joining runs, profiles, summaries, artifacts, and errors.
- `service.py`: report orchestration, workload metric extraction, HammerDB artifact fallback parsing, aggregation, and overhead calculation.
- `host_metrics.py`: Windows PerfMon CSV parsing and per-run CPU/memory summary calculation.
- `markdown_renderer.py`: Markdown report rendering.
- `html_renderer.py`: dependency-free HTML and inline SVG graph rendering.
- `csv_exporter.py`: CSV export writing.
- `output_writer.py`: directory creation and multi-format report output dispatch.
- `models.py`: report source rows, per-run rows, aggregate rows, overhead rows, failure rows, and report document models.
- `dto.py`: request and result DTOs for report generation.
- `constants.py`: output filenames, section names, common metric keys, and audit-mode constants.

Metric behavior:

- Prefer `run_summaries.metrics_json["workload"]`.
- If TPM/NOPM are missing and artifact fallback is enabled, parse registered workload/HammerDB artifacts and `<output_dir>/hammerdb_stdout.txt` for `key=value` lines.
- Parse registered `host_metrics_csv` artifacts for total CPU, SQL Server process CPU, available memory, derived memory used, and SQL Server working set metrics.
- Only read artifacts under `ReportGenerationRequest.resolved_artifact_root`; the CLI defaults this to the SQLite DB parent and exposes `--artifact-root` for external output roots.
- Cache parsed host metric samples in `host_metrics_cache.json` under the CSV output directory, keyed by artifact path, size, mtime, and target memory.
- Generate an HTML report with dependency-free inline SVG charts for per-VU audit mode comparisons and per-run host metric time series.
- Failed, skipped, and incomplete runs remain visible in diagnostics but are excluded from aggregates and audit overhead calculations.
