# Project Context

This repository is a Python benchmark orchestration framework for measuring the performance impact of database audit logging. The current real-run architecture targets SQL Server 2019 on Windows with HammerDB on a separate client VM, connected through a FastAPI SQL Server VM agent. The code is shaped for later Postgres, Linux, SSH, pgbench, reporting, and SQLite dashboard work.

Current implementation status:

- Domain models and run state live in `orchestration/`.
- Structured benchmark config and run-matrix generation live in `config/`.
- SQLite schema and persistence live in `db/`.
- Adapter contracts, agent-backed adapters, local transport, and HammerDB runner live in `adapters/`.
- FastAPI SQL Server agent and HTTP client live in `agents/sqlserver/`.
- SQL/TCL/PowerShell asset generation lives in `scripts/`.
- Markdown/CSV report generation over SQLite results and run artifacts lives in `reporting/`.
- VM-facing JSON examples live in `examples/`.
- Tests use `unittest`, FastAPI `TestClient`, HTTPX mock transports, and fake adapters under `tests/`.

Important constraints:

- Keep dependencies pinned in `requirements.txt`; FastAPI, Uvicorn, Pydantic, and HTTPX are required for real VM operation.
- Keep domain models free of persistence, transport, or infrastructure concerns.
- Keep SQL centralized in `db/`.
- Keep orchestration dependent on adapter abstractions, not concrete SQL Server agent, Windows, or HammerDB classes.
- The SQL Server agent must expose whitelisted actions only, not a generic shell endpoint.
- Do not require real remote infrastructure in tests.

Verification commands:

- `env/bin/python -m unittest discover -s tests`
- `env/bin/python -m compileall agents adapters config orchestration db scripts reporting tests benchpress_orchestrator.py sqlserver_agent.py generate_benchmark_assets.py generate_benchmark_report.py`

Reporting workflow:

- `python -m generate_benchmark_report --db benchpress.sqlite3 --out reports/summary2304-test1.md`
- Exports `runs.csv`, `aggregates.csv`, `overhead.csv`, and `failures.csv` beside the Markdown report by default.
- Excludes failed/skipped/incomplete runs from aggregates while keeping them visible in diagnostics.

Known follow-up gaps:

- Validate generated SQL Server audit, HammerDB TCL, and logman scripts on real benchmark VMs.
- Add chart generation and dashboard layers over SQLite data.
