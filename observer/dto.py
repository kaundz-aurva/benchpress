from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from observer.constants import DEFAULT_PREVIEW_BYTES, DEFAULT_REFRESH_SECONDS


def _positive_float(value: float, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or float(value) <= 0:
        raise ValueError(f"{field_name} must be a positive number")


def _positive_int(value: int, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field_name} must be a positive integer")


@dataclass(frozen=True)
class ObserverSessionConfig:
    db_path: Path
    artifact_root: Path
    refresh_seconds: float = DEFAULT_REFRESH_SECONDS
    include_artifact_fallback: bool = True
    preview_bytes: int = DEFAULT_PREVIEW_BYTES

    def __post_init__(self) -> None:
        object.__setattr__(self, "db_path", Path(self.db_path))
        object.__setattr__(self, "artifact_root", Path(self.artifact_root))
        _positive_float(self.refresh_seconds, "refresh_seconds")
        _positive_int(self.preview_bytes, "preview_bytes")
