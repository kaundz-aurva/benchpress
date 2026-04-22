# Database Persistence Context

This folder owns SQLite persistence for benchmark metadata, run state, artifacts, summaries, and errors.

Key files:

- `constants.py`: table names.
- `migrations.py`: deterministic schema creation with `sqlite3`.
- `repository.py`: typed create/get/update flows.

Rules:

- Keep all SQL in this folder.
- Use parameterized SQL only.
- Use `sqlite3` from the Python standard library.
- Return domain models from repository methods when practical.
- Keep schema creation centralized in `initialize_schema`.

Current tables:

- `benchmark_profiles`
- `hosts`
- `workload_profiles`
- `audit_profiles`
- `runs`
- `run_artifacts`
- `run_summaries`
- `run_errors`

