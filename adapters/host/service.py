from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from orchestration.models import RunArtifact


class HostAdapter(ABC):
    @abstractmethod
    def start_metrics_collection(self, run_id: int, output_dir: Path) -> None:
        raise NotImplementedError

    @abstractmethod
    def stop_metrics_collection(self, run_id: int, output_dir: Path) -> list[RunArtifact]:
        raise NotImplementedError

    @abstractmethod
    def collect_filesystem_stats(self) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def collect_host_metadata(self) -> dict[str, Any]:
        raise NotImplementedError

