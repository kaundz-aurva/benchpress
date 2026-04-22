from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from config.constants import (
    DEFAULT_COOLDOWN_MINUTES,
    DEFAULT_MEASURED_MINUTES,
    DEFAULT_OUTPUT_ROOT,
    DEFAULT_REPETITIONS,
    DEFAULT_VU_LADDER,
    DEFAULT_WARMUP_MINUTES,
    DEFAULT_WORKLOAD_TOOL,
)
from orchestration.models import (
    AuditProfile,
    BenchmarkProfile,
    HostDefinition,
    HostRole,
    _metadata_dict,
    _non_negative_int,
    _positive_int,
    _required_text,
)


@dataclass(frozen=True)
class RunTimingConfig:
    warmup_minutes: int = DEFAULT_WARMUP_MINUTES
    measured_minutes: int = DEFAULT_MEASURED_MINUTES
    cooldown_minutes: int = DEFAULT_COOLDOWN_MINUTES

    def __post_init__(self) -> None:
        _non_negative_int(self.warmup_minutes, "warmup_minutes")
        _positive_int(self.measured_minutes, "measured_minutes")
        _non_negative_int(self.cooldown_minutes, "cooldown_minutes")


@dataclass(frozen=True)
class BenchmarkConfig:
    benchmark_profile: BenchmarkProfile
    target_host: HostDefinition
    client_host: HostDefinition
    audit_profiles: tuple[AuditProfile, ...]
    virtual_user_ladder: tuple[int, ...] = DEFAULT_VU_LADDER
    repetitions: int = DEFAULT_REPETITIONS
    timings: RunTimingConfig = field(default_factory=RunTimingConfig)
    output_root: Path = Path(DEFAULT_OUTPUT_ROOT)
    workload_tool: str = DEFAULT_WORKLOAD_TOOL
    workload_metadata: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.target_host.role is not HostRole.TARGET:
            raise ValueError("target_host must have role target")
        if self.client_host.role is not HostRole.CLIENT:
            raise ValueError("client_host must have role client")
        if not self.audit_profiles:
            raise ValueError("audit_profiles must not be empty")
        _positive_int(self.repetitions, "repetitions")
        _required_text(self.workload_tool, "workload_tool")
        for virtual_users in self.virtual_user_ladder:
            _positive_int(virtual_users, "virtual_user_ladder item")
        object.__setattr__(self, "audit_profiles", tuple(self.audit_profiles))
        object.__setattr__(self, "virtual_user_ladder", tuple(self.virtual_user_ladder))
        object.__setattr__(self, "output_root", Path(self.output_root))
        object.__setattr__(
            self,
            "workload_metadata",
            _metadata_dict(self.workload_metadata, "workload_metadata"),
        )

