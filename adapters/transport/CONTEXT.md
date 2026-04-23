# Transport Adapter Context

This folder owns the remote execution and file transfer boundary.

Key files:

- `service.py`: `TransportAdapter` abstract base class.
- `dto.py`: `RemoteCommandRequest` and `RemoteCommandResult`.

`local/` provides `LocalTransport` for running HammerDB commands on the client VM. SQL Server VM actions use the FastAPI agent instead of a remote-shell transport in this slice.

Rules:

- Keep command request/result DTOs simple.
- Preserve timeout, stdout, stderr, exit code, and timed-out state.
- Do not make tests depend on real networks or remote hosts.
