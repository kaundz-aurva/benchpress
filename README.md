# Benchpress

Benchpress is a Python benchmark orchestration framework for measuring the performance impact of database audit logging. The first target slice is SQL Server 2019 on Windows with HammerDB workloads, while the architecture is kept open for Postgres, Linux, SSH, pgbench, reporting, and a future SQLite-backed dashboard.

## Features

- Typed benchmark domain models for profiles, hosts, workloads, audit modes, runs, artifacts, summaries, and errors.
- Structured config models that generate a run matrix across audit modes, virtual-user ladders, and repetitions.
- SQLite persistence for benchmark metadata, run lifecycle state, artifacts, summaries, and failures.
- Adapter interfaces for database, host, transport, and workload execution.
- First-pass SQL Server, Windows host, and HammerDB adapter implementations.
- Orchestration service that coordinates setup, precheck, metrics, snapshots, workload execution, artifact collection, summarization, and failure persistence.
- Standard-library `unittest` coverage with fake adapters and temporary SQLite databases.

## Requirements

- Python 3.10 or newer.
- No third-party Python packages are currently required.
- Real benchmark execution will require external infrastructure and tooling, such as SQL Server, HammerDB, and a concrete transport adapter. Those are not installed by this repository.

## Installation

Clone the repository, create a virtual environment, and install the requirements file:

```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
```

The current `requirements.txt` is intentionally empty apart from comments because the code uses only the Python standard library. It was checked against `pip freeze`; the only installed package in this environment was `wheel`, which is not required by Benchpress.

## Usage

Run the test suite:

```bash
python3 -m unittest discover -s tests
```

Run a syntax check:

```bash
python3 -m compileall config orchestration db adapters tests
```

Initialize a SQLite repository:

```python
from pathlib import Path

from db.repository import BenchmarkRepository

repo = BenchmarkRepository(Path("benchpress.sqlite3"))
repo.create_schema()
```

Generate a benchmark run matrix:

```python
from pathlib import Path

from config.models import BenchmarkConfig
from config.service import BenchmarkConfigService
from orchestration.models import AuditProfile, BenchmarkProfile, HostDefinition, HostRole

config = BenchmarkConfig(
    benchmark_profile=BenchmarkProfile(name="sqlserver-audit"),
    target_host=HostDefinition("sql", HostRole.TARGET, "windows", "sql-host", 4, 16),
    client_host=HostDefinition("client", HostRole.CLIENT, "windows", "client-host", 2, 4),
    audit_profiles=(AuditProfile("off", "audit_off"), AuditProfile("on", "audit_on")),
    output_root=Path("outputs"),
)

run_specs = BenchmarkConfigService().build_run_matrix(config)
```

To execute real runs, provide concrete adapters for database, host, transport, and workload behavior, then pass them into `BenchmarkOrchestrationService`. The current tests show fake adapter examples that do not require remote infrastructure.

## Project Layout

- `config/`: benchmark configuration models and run-matrix generation.
- `orchestration/`: domain models, DTOs, constants, and lifecycle service.
- `db/`: SQLite schema creation and repository methods.
- `adapters/`: extension seams plus SQL Server, Windows, and HammerDB scaffolding.
- `tests/`: standard-library unit tests.

## Development Notes

Keep domain models infrastructure-neutral, keep SQL inside `db/`, and keep orchestration dependent on adapter interfaces rather than concrete infrastructure classes. Tests should stay isolated and must not require real cloud, database, Windows, HammerDB, or network resources.

