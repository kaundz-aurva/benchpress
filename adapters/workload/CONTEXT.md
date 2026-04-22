# Workload Adapter Context

This folder owns workload runner contracts and DTOs.

Key files:

- `service.py`: `WorkloadRunner` abstract base class.
- `dto.py`: `WorkloadExecutionRequest` and `WorkloadExecutionResult`.
- `constants.py`: workload tool constants.
- `hammerdb/`: first concrete HammerDB implementation.

The workload seam exists so future work can add `PgBenchRunner` or other workload tools without changing orchestration logic.

Responsibilities:

- Prepare a run.
- Execute a run.
- Parse results into typed summary metrics.

