from __future__ import annotations

from orchestration.models import RunPhase, RunStatus


RUN_PHASE_SEQUENCE: tuple[RunPhase, ...] = (
    RunPhase.SETUP,
    RunPhase.PRECHECK,
    RunPhase.METRICS_START,
    RunPhase.PRE_SNAPSHOT,
    RunPhase.WORKLOAD_RUN,
    RunPhase.POST_SNAPSHOT,
    RunPhase.METRICS_STOP,
    RunPhase.ARTIFACT_COLLECTION,
    RunPhase.SUMMARIZE,
    RunPhase.DONE,
)

TERMINAL_STATUSES: tuple[RunStatus, ...] = (
    RunStatus.SUCCESS,
    RunStatus.FAILED,
    RunStatus.SKIPPED,
)

