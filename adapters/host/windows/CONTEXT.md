# Windows Host Adapter Context

This folder contains the first-pass Windows host adapter for SQL Server benchmark VMs.

Current behavior:

- Requires `HostDefinition.os_type == "windows"`.
- Can start and stop metrics collection through configured commands.
- Writes stop-metrics output as a run artifact.
- Provides basic host metadata from `HostDefinition`.
- Requires a configured command for filesystem stats.

Future work:

- Add real Windows metrics commands.
- Add disk, CPU, memory, and perf counter collection.
- Add clear artifact naming for collected Windows performance logs.

