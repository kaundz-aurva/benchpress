from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from reporting.models import HostMetricSample, ReportArtifact, ReportSourceRun


@dataclass(frozen=True)
class ObserverRunState:
    source_run: ReportSourceRun
    workload_metrics: dict[str, Any] = field(default_factory=dict)
    host_metrics: dict[str, float | int] = field(default_factory=dict)
    host_samples: tuple[HostMetricSample, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "workload_metrics", dict(self.workload_metrics))
        object.__setattr__(self, "host_metrics", dict(self.host_metrics))
        object.__setattr__(self, "host_samples", tuple(self.host_samples))

    @property
    def run_id(self) -> int:
        return self.source_run.run_id

    @property
    def benchmark_name(self) -> str:
        return self.source_run.benchmark_name

    @property
    def workload_name(self) -> str:
        return self.source_run.workload_name

    @property
    def workload_tool(self) -> str:
        return self.source_run.workload_tool

    @property
    def audit_mode(self) -> str:
        return self.source_run.audit_mode

    @property
    def virtual_users(self) -> int:
        return self.source_run.virtual_users

    @property
    def repetition(self) -> int:
        return self.source_run.repetition

    @property
    def status(self) -> str:
        return self.source_run.status

    @property
    def phase(self) -> str:
        return self.source_run.phase

    @property
    def created_at(self) -> str:
        return self.source_run.created_at

    @property
    def updated_at(self) -> str:
        return self.source_run.updated_at

    @property
    def output_dir(self) -> Path:
        return self.source_run.output_dir

    @property
    def target_memory_gb(self) -> int:
        return self.source_run.target_memory_gb

    @property
    def summary_notes(self) -> str:
        return self.source_run.summary_notes

    @property
    def summary_metrics(self) -> dict[str, Any]:
        return dict(self.source_run.summary_metrics)

    @property
    def summary_metadata(self) -> dict[str, Any]:
        return {
            key: value
            for key, value in self.source_run.summary_metrics.items()
            if key != "workload"
        }

    @property
    def artifacts(self) -> tuple[ReportArtifact, ...]:
        return self.source_run.artifacts

    @property
    def errors(self):
        return self.source_run.errors

    @property
    def has_failures(self) -> bool:
        return bool(self.errors) or self.status != "success"


@dataclass(frozen=True)
class ObserverSnapshot:
    db_path: Path
    artifact_root: Path
    collected_at: str
    runs: tuple[ObserverRunState, ...]
    status_counts: dict[str, int] = field(default_factory=dict)
    phase_counts: dict[str, int] = field(default_factory=dict)
    active_runs: tuple[ObserverRunState, ...] = ()
    recent_failures: tuple[ObserverRunState, ...] = ()
    latest_updated_runs: tuple[ObserverRunState, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "db_path", Path(self.db_path))
        object.__setattr__(self, "artifact_root", Path(self.artifact_root))
        object.__setattr__(self, "runs", tuple(self.runs))
        object.__setattr__(self, "status_counts", dict(self.status_counts))
        object.__setattr__(self, "phase_counts", dict(self.phase_counts))
        object.__setattr__(self, "active_runs", tuple(self.active_runs))
        object.__setattr__(self, "recent_failures", tuple(self.recent_failures))
        object.__setattr__(self, "latest_updated_runs", tuple(self.latest_updated_runs))

    def find_run(self, run_id: int) -> ObserverRunState | None:
        for run in self.runs:
            if run.run_id == run_id:
                return run
        return None


@dataclass(frozen=True)
class ArtifactPreview:
    artifact: ReportArtifact
    resolved_path: Path | None
    previewable: bool
    text: str = ""
    reason: str = ""
    truncated: bool = False

    def __post_init__(self) -> None:
        if self.resolved_path is not None:
            object.__setattr__(self, "resolved_path", Path(self.resolved_path))
