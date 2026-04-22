from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from orchestration.models import AuditProfile, HostDefinition, WorkloadProfile, _positive_int


@dataclass(frozen=True)
class WorkloadExecutionRequest:
    run_id: int
    workload_profile: WorkloadProfile
    target_host: HostDefinition
    client_host: HostDefinition
    audit_profile: AuditProfile
    output_dir: Path

    def __post_init__(self) -> None:
        _positive_int(self.run_id, "run_id")
        object.__setattr__(self, "output_dir", Path(self.output_dir))


@dataclass(frozen=True)
class WorkloadExecutionResult:
    success: bool
    artifacts: tuple[Path, ...] = ()
    metrics: dict[str, Any] = field(default_factory=dict)
    raw_output_path: Path | None = None
    error_message: str = ""

    def __post_init__(self) -> None:
        object.__setattr__(self, "artifacts", tuple(Path(path) for path in self.artifacts))
        object.__setattr__(self, "metrics", dict(self.metrics))
        if self.raw_output_path is not None:
            object.__setattr__(self, "raw_output_path", Path(self.raw_output_path))
        if not self.success and not self.error_message:
            raise ValueError("error_message is required when success is false")

