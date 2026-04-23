# Examples Context

This folder contains JSON templates for real VM execution.

Files:

- `benchmark_spec.example.json`: client-VM orchestrator spec.
- `sqlserver_agent.example.json`: SQL Server VM agent config.

`benchmark_spec.example.json` also includes an `assets` section for generating:

- SQL Server audit scripts.
- HammerDB SQL Server TPROC-C TCL.
- Windows PerfMon/logman scripts.
- `sqlserver_agent.generated.json`.

Keep examples realistic but non-secret. Do not commit bearer tokens, passwords, host-specific credentials, or production audit details.
