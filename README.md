# Benchpress

Benchpress is a Python benchmark orchestration framework for measuring the performance impact of database audit logging. The current real-run target is SQL Server 2019 on Windows with HammerDB running from a separate client VM. The design keeps database, host, workload, transport, orchestration, and persistence concerns separated so later work can add Postgres, Linux, SSH, pgbench, reporting, and dashboarding.

## Architecture

```text
Client VM
  benchpress_orchestrator.py
  HammerDB
  SQLite results DB
  raw output directory

        HTTP + bearer token

SQL Server VM
  sqlserver_agent.py FastAPI service
  sqlcmd audit/snapshot operations
  Windows metrics commands
  artifact staging directory
```

The orchestrator owns run state and SQLite persistence. The SQL Server agent exposes whitelisted actions only; it does not expose a generic shell endpoint.

## Features

- JSON benchmark specs for real VM runs.
- FastAPI SQL Server VM agent with bearer-token authentication.
- Agent-backed `DatabaseAdapter` and `HostAdapter` implementations.
- Local transport for running HammerDB on the client VM.
- SQLite persistence for benchmark profiles, hosts, workloads, audit modes, runs, artifacts, summaries, and errors.
- Run matrix generation across audit modes, VU ladder, and repetitions.
- Raw artifact collection from both client-side HammerDB and SQL Server VM agent.
- Deterministic generation of SQL Server audit SQL, HammerDB TPROC-C TCL, and Windows PerfMon/logman scripts from JSON.

## Installation

On both VMs:

```bash
python3 -m venv env
source env/bin/activate
python -m pip install -r requirements.txt
```

On Windows PowerShell:

```powershell
py -3 -m venv env
.\env\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

## Generate Benchmark Assets

Edit `examples/benchmark_spec.example.json`, especially:

- `assets.audit.audit_file_path`
- `assets.audit.selected_databases`
- `assets.hammerdb.sql_server`
- `assets.hammerdb.database_name`
- `assets.metrics.output_root`

Generate SQL, TCL, PowerShell, and agent config files:

```bash
python -m generate_benchmark_assets --spec examples/benchmark_spec.example.json --out generated
```

This writes:

- `audit_enable.sql`
- `audit_disable.sql`
- `audit_snapshot_pre.sql`
- `audit_snapshot_post.sql`
- `audit_metadata.sql`
- `hammerdb_tprocc_sqlserver.tcl`
- `start_metrics.ps1`
- `stop_metrics.ps1`
- `sqlserver_agent.generated.json`

Default audit object names are:

- `Audit-benchpress`
- `Server-Audit-Spec-benchpress`
- `Db-Audit-Spec-benchpress-{database}`

## SQL Server VM Setup

Install SQL Server 2019 command-line access (`sqlcmd`). Copy the generated SQL and PowerShell files to the SQL Server VM at the paths referenced by `sqlserver_agent.generated.json`.

```text
generated/sqlserver_agent.generated.json
```

Set the bearer token:

```powershell
$env:BENCHPRESS_AGENT_TOKEN = "replace-with-a-long-random-token"
```

Start the SQL Server agent:

```powershell
python -m sqlserver_agent --config generated/sqlserver_agent.generated.json --host 0.0.0.0 --port 8080
```

Firewall the port so only the client VM can reach it.

## Client VM Run

Install HammerDB and copy the generated TCL script to the path configured by `workload.hammerdb_script_path`. Create or edit a benchmark spec based on:

```text
examples/benchmark_spec.example.json
```

Set the same bearer token:

```bash
export BENCHPRESS_AGENT_TOKEN="replace-with-a-long-random-token"
```

Run the benchmark:

```bash
python -m benchpress_orchestrator --spec examples/benchmark_spec.example.json
```

The orchestrator creates the SQLite DB configured by `storage.sqlite_path` and writes raw run artifacts under `storage.output_root`.

## Generate Reports

After a run, generate a Markdown report and CSV exports from the SQLite database:

```bash
python -m generate_benchmark_report --db benchpress.sqlite3 --out reports/summary2304-test1.md
```

By default, CSV files are written under the Markdown file's sibling `csv/` directory:

- `runs.csv`: one row per run with dimensions, status, phase, output directory, and flattened workload metrics.
- `aggregates.csv`: successful runs grouped by audit mode and virtual user count.
- `overhead.csv`: `audit_on` compared against `audit_off` for matching virtual user counts.
- `failures.csv`: persisted errors plus failed, skipped, or incomplete runs.

The report prefers workload metrics from `run_summaries.metrics_json["workload"]`. If TPM/NOPM are missing, it also checks registered workload artifacts such as `hammerdb_stdout.txt` for `key=value` metrics unless disabled:

```bash
python -m generate_benchmark_report --db benchpress.sqlite3 --out reports/summary.md --no-artifact-fallback
```

Failed, skipped, and incomplete runs are shown in diagnostics but excluded from aggregate and audit overhead calculations. For TPM/NOPM, a negative percent change means `audit_on` throughput was lower than `audit_off`.

## Development

Run tests:

```bash
env/bin/python -m unittest discover -s tests
```

Run a syntax check:

```bash
env/bin/python -m compileall agents adapters config orchestration db scripts reporting tests benchpress_orchestrator.py sqlserver_agent.py generate_benchmark_assets.py generate_benchmark_report.py
```

Tests use fakes, FastAPI `TestClient`, temp directories, and temp SQLite DBs. They do not require real SQL Server, HammerDB, Windows VMs, or network access.

## Project Layout

- `agents/`: FastAPI SQL Server VM agent and HTTP client.
- `adapters/`: database, host, transport, and workload adapter seams plus concrete implementations.
- `config/`: JSON runtime spec DTOs and run-matrix configuration.
- `scripts/`: asset generation for SQL audit, HammerDB, and Windows metrics files.
- `reporting/`: Markdown and CSV report generation from SQLite results and run artifacts.
- `orchestration/`: domain models and benchmark lifecycle service.
- `db/`: SQLite schema and repository.
- `examples/`: real-run JSON spec templates.
- `tests/`: isolated unit and integration-style tests.
