from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ReportArtifact:
    artifact_id: int
    run_id: int
    artifact_type: str
    path: Path
    description: str
    created_at: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "path", Path(self.path))


@dataclass(frozen=True)
class ReportError:
    error_id: int
    run_id: int
    phase: str
    message: str
    exception_type: str
    created_at: str


@dataclass(frozen=True)
class ReportSourceRun:
    run_id: int
    benchmark_name: str
    database_engine: str
    database_version: str
    cloud_provider: str
    workload_name: str
    workload_tool: str
    virtual_users: int
    repetition: int
    audit_name: str
    audit_mode: str
    status: str
    phase: str
    output_dir: Path
    created_at: str
    updated_at: str
    summary_metrics: dict[str, Any] = field(default_factory=dict)
    summary_notes: str = ""
    artifacts: tuple[ReportArtifact, ...] = ()
    errors: tuple[ReportError, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "output_dir", Path(self.output_dir))
        object.__setattr__(self, "summary_metrics", dict(self.summary_metrics))
        object.__setattr__(self, "artifacts", tuple(self.artifacts))
        object.__setattr__(self, "errors", tuple(self.errors))


@dataclass(frozen=True)
class ReportRunRow:
    run_id: int
    benchmark_name: str
    workload_name: str
    workload_tool: str
    audit_mode: str
    virtual_users: int
    repetition: int
    status: str
    phase: str
    output_dir: Path
    created_at: str
    updated_at: str
    summary_notes: str
    workload_metrics: dict[str, Any] = field(default_factory=dict)
    artifacts: tuple[ReportArtifact, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "output_dir", Path(self.output_dir))
        object.__setattr__(self, "workload_metrics", dict(self.workload_metrics))
        object.__setattr__(self, "artifacts", tuple(self.artifacts))


@dataclass(frozen=True)
class MetricStats:
    count: int
    mean: float
    minimum: float
    maximum: float


@dataclass(frozen=True)
class AggregateRow:
    audit_mode: str
    virtual_users: int
    run_count: int
    metrics: dict[str, MetricStats] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "metrics", dict(self.metrics))


@dataclass(frozen=True)
class OverheadRow:
    virtual_users: int
    metric_name: str
    audit_off_mean: float
    audit_on_mean: float
    delta: float
    percent_change: float | None


@dataclass(frozen=True)
class FailureRow:
    run_id: int
    audit_mode: str
    virtual_users: int
    repetition: int
    phase: str
    status: str
    exception_type: str
    message: str


@dataclass(frozen=True)
class ReportDocument:
    db_path: Path
    generated_at: str
    source_runs: tuple[ReportSourceRun, ...]
    runs: tuple[ReportRunRow, ...]
    aggregates: tuple[AggregateRow, ...]
    overhead: tuple[OverheadRow, ...]
    failures: tuple[FailureRow, ...]

    def __post_init__(self) -> None:
        object.__setattr__(self, "db_path", Path(self.db_path))
        object.__setattr__(self, "source_runs", tuple(self.source_runs))
        object.__setattr__(self, "runs", tuple(self.runs))
        object.__setattr__(self, "aggregates", tuple(self.aggregates))
        object.__setattr__(self, "overhead", tuple(self.overhead))
        object.__setattr__(self, "failures", tuple(self.failures))
