# Adapter Context

This folder contains extension seams for infrastructure-specific behavior.

Adapter groups:

- `database/`: database audit, metadata, sanity check, and snapshot behavior.
- `host/`: OS and host-level metrics and metadata behavior.
- `transport/`: remote command and file movement boundary.
- `workload/`: workload preparation, execution, and result parsing.

Rules:

- Keep adapter interfaces small and stable.
- Concrete adapters should validate constructor configuration early.
- Orchestration should depend on these abstractions, not concrete implementations.
- Tests should use fakes instead of real remote infrastructure.

Current concrete implementations include SQL Server/Windows direct stubs, SQL Server agent-backed adapters, local transport for client VM execution, and HammerDB workload execution.
