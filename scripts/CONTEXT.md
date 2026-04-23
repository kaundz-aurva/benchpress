# Script Generation Context

This folder owns deterministic generation of real-VM assets from the benchmark JSON spec.

Generated assets:

- SQL Server audit enable/disable/snapshot/metadata SQL files.
- HammerDB SQL Server TPROC-C TCL workload file.
- Windows PerfMon/logman start and stop PowerShell scripts.
- SQL Server agent JSON config pointing at generated files.

Keep generated script logic here. Runtime orchestration should consume generated files through existing agent and workload configuration instead of embedding script text.
