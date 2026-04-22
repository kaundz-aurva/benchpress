from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from orchestration.models import HostDefinition, _positive_int, _required_text


@dataclass(frozen=True)
class SnapshotRequest:
    run_id: int
    host: HostDefinition
    output_dir: Path
    label: str

    def __post_init__(self) -> None:
        _positive_int(self.run_id, "run_id")
        _required_text(self.label, "label")
        object.__setattr__(self, "output_dir", Path(self.output_dir))

