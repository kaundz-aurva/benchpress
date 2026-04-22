# Project Context

This repository is a Python benchmark orchestration framework for measuring the performance impact of database audit logging. The first supported target is SQL Server 2019 on Windows with HammerDB as the workload runner, but the code is intentionally shaped for later Postgres, Linux, SSH, pgbench, reporting, and SQLite dashboard work.

Current implementation status:

- Domain models and run state live in `orchestration/`.
- Structured benchmark config and run-matrix generation live in `config/`.
- SQLite schema and persistence live in `db/`.
- Adapter contracts and first concrete SQL Server, Windows, and HammerDB implementations live in `adapters/`.
- Tests use standard-library `unittest` and fake adapters under `tests/`.

Important constraints:

- Use only the Python standard library unless a future change explicitly introduces package management.
- Keep domain models free of persistence, transport, or infrastructure concerns.
- Keep SQL centralized in `db/`.
- Keep orchestration dependent on adapter abstractions, not concrete SQL Server, Windows, or HammerDB classes.
- Do not require real remote infrastructure in tests.

Verification commands:

- `python3 -m unittest discover -s tests`
- `python3 -m compileall config orchestration db adapters tests`

Known follow-up gaps:

- Add a real transport adapter, likely WinRM first and SSH later.
- Add production SQL Server audit scripts and snapshot queries.
- Add reporting, chart generation, and dashboard layers over SQLite data.

