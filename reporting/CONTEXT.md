# Reporting Context

The reporting package turns an existing Benchpress SQLite database and registered run artifacts into human-readable Markdown plus CSV exports. It is read-only with respect to `benchpress.sqlite3`; it does not create or migrate schema.

Main modules:

- `repository.py`: SQLite read queries joining runs, profiles, summaries, artifacts, and errors.
- `service.py`: workload metric extraction, HammerDB artifact fallback parsing, aggregation, overhead calculation, Markdown rendering, and CSV writing.
- `models.py`: report source rows, per-run rows, aggregate rows, overhead rows, failure rows, and report document models.
- `dto.py`: request and result DTOs for report generation.
- `constants.py`: output filenames, section names, common metric keys, and audit-mode constants.

Metric behavior:

- Prefer `run_summaries.metrics_json["workload"]`.
- If TPM/NOPM are missing and artifact fallback is enabled, parse registered workload/HammerDB artifacts and `<output_dir>/hammerdb_stdout.txt` for `key=value` lines.
- Failed, skipped, and incomplete runs remain visible in diagnostics but are excluded from aggregates and audit overhead calculations.
