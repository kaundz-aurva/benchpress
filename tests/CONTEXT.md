# Tests Context

Tests use Python standard-library `unittest`; no third-party test framework is configured.

Coverage currently includes:

- Domain model validation and enum behavior.
- DTO defaults and required field validation.
- SQLite schema, repository create/get/update flows, artifacts, summaries, and errors.
- Adapter constructor validation, happy paths, and explicit unsupported behavior.
- Config validation and run-matrix generation.
- Orchestration success and failure paths using fake adapters.

Rules:

- Use temp directories and temp SQLite DBs.
- Do not require real SQL Server, Windows VMs, HammerDB, network access, or cloud resources.
- Prefer fake adapters over mocks for orchestration tests.

Run tests with:

- `python3 -m unittest discover -s tests`

