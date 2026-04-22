# Transport Adapter Context

This folder owns the remote execution and file transfer boundary.

Key files:

- `service.py`: `TransportAdapter` abstract base class.
- `dto.py`: `RemoteCommandRequest` and `RemoteCommandResult`.

No concrete transport exists yet. SQL Server, Windows host, and HammerDB implementations use this seam so future work can add WinRM, SSH, or local transports without changing orchestration logic.

Rules:

- Keep command request/result DTOs simple.
- Preserve timeout, stdout, stderr, exit code, and timed-out state.
- Do not make tests depend on real networks or remote hosts.

