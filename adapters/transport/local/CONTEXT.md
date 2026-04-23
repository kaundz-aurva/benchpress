# Local Transport Context

This folder contains `LocalTransport`, used by the client-VM orchestrator to execute HammerDB locally.

Behavior:

- Runs command strings with local subprocess execution.
- Supports local file copy for upload/download semantics.
- Treats connectivity as local and always available.

Do not use this transport for SQL Server VM operations; those go through the FastAPI SQL Server agent.

