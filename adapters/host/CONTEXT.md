# Host Adapter Context

This folder owns host and OS adapter contracts.

Key files:

- `service.py`: `HostAdapter` abstract base class.
- `constants.py`: OS constants.
- `windows/`: first concrete Windows implementation.

The host adapter seam exists so future work can add `LinuxHostAdapter` without changing orchestration logic.

Responsibilities:

- Start and stop metrics collection.
- Collect filesystem stats.
- Collect host metadata.

Keep database-specific and workload-specific behavior out of host adapters.

