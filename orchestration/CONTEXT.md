# Orchestration Context

This folder owns benchmark domain models, lifecycle DTOs, and the central orchestration service.

Key files:

- `models.py`: domain dataclasses and enums such as `BenchmarkProfile`, `RunSpec`, `RunRecord`, `RunStatus`, and `RunPhase`.
- `dto.py`: orchestration boundary DTOs for run creation, updates, and artifact registration.
- `constants.py`: explicit phase sequence and terminal statuses.
- `service.py`: `BenchmarkOrchestrationService`.

Design rules:

- Keep models serializable and infrastructure-neutral.
- Use explicit `RunStatus` and `RunPhase` enums instead of scattered strings.
- Persist every meaningful run transition through the repository.
- Catch lifecycle failures, persist `ErrorRecord`, and leave final run state observable.
- Depend on adapter interfaces from `adapters/`, not concrete SQL Server, Windows, or HammerDB implementations.

The current service supports real single-run lifecycle execution when supplied with agent-backed SQL Server/Windows adapters and a HammerDB runner. VM entrypoint wiring lives in `benchpress_orchestrator.py`.
