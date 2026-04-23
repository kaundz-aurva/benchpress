# Tests Context

Tests use Python standard-library `unittest`; no third-party test framework is configured.

Coverage currently includes:

- Domain model validation and enum behavior.
- DTO defaults and required field validation.
- SQLite schema, repository create/get/update flows, artifacts, summaries, and errors.
- Adapter constructor validation, happy paths, and explicit unsupported behavior.
- Config validation and run-matrix generation.
- Orchestration success and failure paths using fake adapters.
- FastAPI SQL Server agent routes using `TestClient`.
- Agent HTTP client and agent-backed adapters using HTTPX mock transports.
- Client-VM orchestrator entrypoint using fake agent/workload implementations.
- Asset generation for SQL Server audit, HammerDB TPROC-C, and Windows logman scripts.

Rules:

- Use temp directories and temp SQLite DBs.
- Do not require real SQL Server, Windows VMs, HammerDB, network access, or cloud resources.
- Prefer fake adapters over mocks for orchestration tests.

Run tests with:

- `env/bin/python -m unittest discover -s tests`

Do not add tests that require real SQL Server, real HammerDB, or real VM networking.
