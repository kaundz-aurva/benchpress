# Database Adapter Context

This folder owns database adapter contracts and database-specific DTOs.

Key files:

- `service.py`: `DatabaseAdapter` abstract base class.
- `dto.py`: `SnapshotRequest`.
- `constants.py`: database engine/version constants.
- `sqlserver/`: first concrete SQL Server implementation.

The database adapter seam exists so future work can add `PostgresDatabaseAdapter` without changing orchestration logic.

Required responsibilities:

- Connectivity validation.
- Audit enable/disable hooks.
- Sanity checks.
- Pre/post snapshots.
- Database metadata collection.

Do not place transport-specific command execution policies in orchestration.

