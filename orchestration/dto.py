from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from orchestration.models import RunPhase, RunSpec, RunStatus


def _positive_run_id(run_id: int) -> None:
    if isinstance(run_id, bool) or not isinstance(run_id, int) or run_id <= 0:
        raise ValueError("run_id must be a positive integer")


@dataclass(frozen=True)
class ArtifactRegistrationRequest:
    run_id: int
    artifact_type: str
    path: Path
    description: str = ""

    def __post_init__(self) -> None:
        _positive_run_id(self.run_id)
        if not self.artifact_type.strip():
            raise ValueError("artifact_type is required")
        object.__setattr__(self, "path", Path(self.path))


@dataclass(frozen=True)
class RunCreationRequest:
    run_spec: RunSpec

    def __post_init__(self) -> None:
        if not isinstance(self.run_spec, RunSpec):
            raise ValueError("run_spec must be a RunSpec")


@dataclass(frozen=True)
class RunUpdateRequest:
    run_id: int
    status: RunStatus | str | None = None
    phase: RunPhase | str | None = None

    def __post_init__(self) -> None:
        _positive_run_id(self.run_id)
        if self.status is None and self.phase is None:
            raise ValueError("status or phase is required")
        if self.status is not None:
            object.__setattr__(self, "status", RunStatus(self.status))
        if self.phase is not None:
            object.__setattr__(self, "phase", RunPhase(self.phase))

