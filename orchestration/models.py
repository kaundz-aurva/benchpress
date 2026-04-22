from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _required_text(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} is required")


def _positive_int(value: int, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field_name} must be a positive integer")


def _non_negative_int(value: int, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{field_name} must be a non-negative integer")


def _optional_positive_int(value: int | None, field_name: str) -> None:
    if value is not None:
        _positive_int(value, field_name)


def _metadata_dict(value: dict[str, Any], field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be a dictionary")
    return dict(value)


class AuditMode(str, Enum):
    AUDIT_OFF = "audit_off"
    AUDIT_ON = "audit_on"


class HostRole(str, Enum):
    TARGET = "target"
    CLIENT = "client"


class RunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class RunPhase(str, Enum):
    SETUP = "setup"
    PRECHECK = "precheck"
    METRICS_START = "metrics_start"
    PRE_SNAPSHOT = "pre_snapshot"
    WORKLOAD_RUN = "workload_run"
    POST_SNAPSHOT = "post_snapshot"
    METRICS_STOP = "metrics_stop"
    ARTIFACT_COLLECTION = "artifact_collection"
    SUMMARIZE = "summarize"
    DONE = "done"


@dataclass(frozen=True)
class BenchmarkProfile:
    name: str
    database_engine: str = "sqlserver"
    database_version: str = "2019"
    cloud_provider: str = "gcp"
    description: str = ""
    profile_id: int | None = None

    def __post_init__(self) -> None:
        _required_text(self.name, "name")
        _required_text(self.database_engine, "database_engine")
        _required_text(self.database_version, "database_version")
        _required_text(self.cloud_provider, "cloud_provider")
        _optional_positive_int(self.profile_id, "profile_id")


@dataclass(frozen=True)
class HostDefinition:
    name: str
    role: HostRole | str
    os_type: str
    hostname: str
    vcpus: int
    memory_gb: int
    cloud_instance_id: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
    host_id: int | None = None

    def __post_init__(self) -> None:
        _required_text(self.name, "name")
        _required_text(self.os_type, "os_type")
        _required_text(self.hostname, "hostname")
        _positive_int(self.vcpus, "vcpus")
        _positive_int(self.memory_gb, "memory_gb")
        _optional_positive_int(self.host_id, "host_id")
        object.__setattr__(self, "role", HostRole(self.role))
        object.__setattr__(self, "metadata", _metadata_dict(self.metadata, "metadata"))


@dataclass(frozen=True)
class WorkloadProfile:
    name: str
    tool: str = "hammerdb"
    virtual_users: int = 10
    warmup_minutes: int = 10
    measured_minutes: int = 20
    cooldown_minutes: int = 5
    metadata: dict[str, Any] = field(default_factory=dict)
    workload_profile_id: int | None = None

    def __post_init__(self) -> None:
        _required_text(self.name, "name")
        _required_text(self.tool, "tool")
        _positive_int(self.virtual_users, "virtual_users")
        _non_negative_int(self.warmup_minutes, "warmup_minutes")
        _positive_int(self.measured_minutes, "measured_minutes")
        _non_negative_int(self.cooldown_minutes, "cooldown_minutes")
        _optional_positive_int(self.workload_profile_id, "workload_profile_id")
        object.__setattr__(self, "metadata", _metadata_dict(self.metadata, "metadata"))


@dataclass(frozen=True)
class AuditProfile:
    name: str
    mode: AuditMode | str
    config: dict[str, Any] = field(default_factory=dict)
    audit_profile_id: int | None = None

    def __post_init__(self) -> None:
        _required_text(self.name, "name")
        _optional_positive_int(self.audit_profile_id, "audit_profile_id")
        object.__setattr__(self, "mode", AuditMode(self.mode))
        object.__setattr__(self, "config", _metadata_dict(self.config, "config"))


@dataclass(frozen=True)
class RunSpec:
    benchmark_profile: BenchmarkProfile
    target_host: HostDefinition
    client_host: HostDefinition
    workload_profile: WorkloadProfile
    audit_profile: AuditProfile
    repetition: int
    output_root: Path

    def __post_init__(self) -> None:
        _positive_int(self.repetition, "repetition")
        if self.target_host.role is not HostRole.TARGET:
            raise ValueError("target_host must have role target")
        if self.client_host.role is not HostRole.CLIENT:
            raise ValueError("client_host must have role client")
        object.__setattr__(self, "output_root", Path(self.output_root))


@dataclass(frozen=True)
class RunRecord:
    benchmark_profile_id: int
    target_host_id: int
    client_host_id: int
    workload_profile_id: int
    audit_profile_id: int
    repetition: int
    output_dir: Path
    status: RunStatus | str = RunStatus.PENDING
    phase: RunPhase | str = RunPhase.SETUP
    created_at: str = field(default_factory=utc_now_iso)
    updated_at: str = field(default_factory=utc_now_iso)
    run_id: int | None = None

    def __post_init__(self) -> None:
        _positive_int(self.benchmark_profile_id, "benchmark_profile_id")
        _positive_int(self.target_host_id, "target_host_id")
        _positive_int(self.client_host_id, "client_host_id")
        _positive_int(self.workload_profile_id, "workload_profile_id")
        _positive_int(self.audit_profile_id, "audit_profile_id")
        _positive_int(self.repetition, "repetition")
        _optional_positive_int(self.run_id, "run_id")
        object.__setattr__(self, "output_dir", Path(self.output_dir))
        object.__setattr__(self, "status", RunStatus(self.status))
        object.__setattr__(self, "phase", RunPhase(self.phase))


@dataclass(frozen=True)
class RunArtifact:
    run_id: int
    artifact_type: str
    path: Path
    description: str = ""
    created_at: str = field(default_factory=utc_now_iso)
    artifact_id: int | None = None

    def __post_init__(self) -> None:
        _positive_int(self.run_id, "run_id")
        _required_text(self.artifact_type, "artifact_type")
        _optional_positive_int(self.artifact_id, "artifact_id")
        object.__setattr__(self, "path", Path(self.path))


@dataclass(frozen=True)
class RunSummary:
    run_id: int
    metrics: dict[str, Any]
    notes: str = ""
    created_at: str = field(default_factory=utc_now_iso)
    summary_id: int | None = None

    def __post_init__(self) -> None:
        _positive_int(self.run_id, "run_id")
        _optional_positive_int(self.summary_id, "summary_id")
        object.__setattr__(self, "metrics", _metadata_dict(self.metrics, "metrics"))


@dataclass(frozen=True)
class ErrorRecord:
    run_id: int
    phase: RunPhase | str
    message: str
    exception_type: str = ""
    created_at: str = field(default_factory=utc_now_iso)
    error_id: int | None = None

    def __post_init__(self) -> None:
        _positive_int(self.run_id, "run_id")
        _required_text(self.message, "message")
        _optional_positive_int(self.error_id, "error_id")
        object.__setattr__(self, "phase", RunPhase(self.phase))

