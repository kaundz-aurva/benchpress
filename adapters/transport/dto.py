from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from orchestration.models import HostDefinition


@dataclass(frozen=True)
class RemoteCommandRequest:
    host: HostDefinition
    command: str
    timeout_seconds: int = 60
    working_dir: Path | None = None
    environment: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.command.strip():
            raise ValueError("command is required")
        if isinstance(self.timeout_seconds, bool) or self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        if self.working_dir is not None:
            object.__setattr__(self, "working_dir", Path(self.working_dir))
        object.__setattr__(self, "environment", dict(self.environment))


@dataclass(frozen=True)
class RemoteCommandResult:
    command: str
    exit_code: int
    stdout: str = ""
    stderr: str = ""
    duration_seconds: float = 0.0
    timed_out: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.exit_code, int):
            raise ValueError("exit_code must be an integer")
        if self.duration_seconds < 0:
            raise ValueError("duration_seconds must be non-negative")

    @property
    def succeeded(self) -> bool:
        return self.exit_code == 0 and not self.timed_out

