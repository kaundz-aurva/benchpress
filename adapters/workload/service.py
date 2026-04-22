from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from adapters.workload.dto import WorkloadExecutionRequest, WorkloadExecutionResult


class WorkloadRunner(ABC):
    @abstractmethod
    def prepare_run(self, request: WorkloadExecutionRequest) -> None:
        raise NotImplementedError

    @abstractmethod
    def execute_run(self, request: WorkloadExecutionRequest) -> WorkloadExecutionResult:
        raise NotImplementedError

    @abstractmethod
    def parse_results(self, result_path: Path) -> dict[str, Any]:
        raise NotImplementedError

