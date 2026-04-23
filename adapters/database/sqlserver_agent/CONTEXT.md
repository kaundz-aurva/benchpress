# SQL Server Agent Adapter Context

This folder contains the orchestrator-side `DatabaseAdapter` that talks to the FastAPI SQL Server VM agent.

Behavior:

- Calls the agent for audit enable/disable, connectivity, sanity checks, snapshots, and database metadata.
- Downloads SQL-side artifacts into the orchestrator run output directory.
- Returns normal orchestration domain models such as `RunArtifact`.

Keep HTTP details here or in `agents/sqlserver/client.py`; do not leak them into `orchestration/`.

