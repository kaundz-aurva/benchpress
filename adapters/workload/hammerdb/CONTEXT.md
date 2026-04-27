# HammerDB Runner Context

This folder contains the first-pass HammerDB workload runner.

Current behavior:

- Validates the workload tool is `hammerdb`.
- Requires a `TransportAdapter` and `script_path` for execution.
- Builds a simple HammerDB auto-mode command without overriding virtual users on the CLI.
- Writes raw stdout to the run output directory and optional stderr alongside it.
- Parses simple `key=value` result lines into metrics.
- Treats usage output, virtual-user failures, SQL Server connection/login failures, or missing `benchmark_status=completed` markers as workload failures.
- The generated SQL Server TCL is expected to target HammerDB 5.0 dictionary keys such as `mssqls_authentication`, `mssqls_dbase`, `mssqls_rampup`, and `mssqls_duration`.

Future work:

- Replace the placeholder command shape with real HammerDB TCL automation.
- Register additional raw output files and generated reports.
- Expand parsing for HammerDB-native output formats.
