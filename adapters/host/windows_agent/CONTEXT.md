# Windows Agent Adapter Context

This folder contains the orchestrator-side `HostAdapter` that calls the SQL Server VM FastAPI agent for Windows host actions.

Behavior:

- Starts and stops metrics collection through agent endpoints.
- Downloads metrics artifacts into the orchestrator output directory.
- Collects filesystem and host metadata through the agent.

Keep Windows-agent HTTP behavior out of the orchestration service.

