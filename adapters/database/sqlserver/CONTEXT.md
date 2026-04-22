# SQL Server Adapter Context

This folder contains the first-pass SQL Server database adapter for the SQL Server 2019 on Windows benchmark target.

Current behavior:

- Requires a `TransportAdapter` at construction time.
- Uses `sqlcmd` command construction with Windows command-line quoting.
- Validates connectivity with `SELECT 1`.
- Requires configured audit scripts for enable/disable operations.
- Requires a configured snapshot query before capturing snapshots.
- Writes snapshot output through `sqlcmd -o` instead of loading snapshot stdout into memory.

Future work:

- Add production audit enable/disable scripts.
- Add robust SQL Server metadata queries.
- Add snapshot query templates for relevant DMVs and audit artifacts.
- Consider richer command execution if the transport grows an argument-list API.

