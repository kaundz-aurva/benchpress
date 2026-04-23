# SQL Server Agent Context

This folder contains the FastAPI agent that runs on the SQL Server VM.

Key files:

- `models.py`: agent config, artifacts, and command result models.
- `dto.py`: Pydantic request/response DTOs for HTTP boundaries.
- `service.py`: SQL, host, filesystem, and artifact providers composed by `SqlServerAgentService`.
- `app.py`: FastAPI app factory and whitelisted endpoints.
- `client.py`: HTTP client used by orchestrator-side adapters.

Security rules:

- Require `Authorization: Bearer <token>` for all non-health endpoints.
- Execute configured commands as argument arrays with `shell=False`.
- Prefer generated SQL files through `*_sql_file` config fields for multi-batch scripts with `GO`.
- Return sanitized command failures to clients.
- Stream artifact downloads to disk from the client side.
