from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path

from adapters.transport.dto import RemoteCommandRequest, RemoteCommandResult
from orchestration.models import HostDefinition


class TransportAdapter(ABC):
    @abstractmethod
    def execute_command(self, request: RemoteCommandRequest) -> RemoteCommandResult:
        raise NotImplementedError

    @abstractmethod
    def upload_file(self, local_path: Path, remote_path: Path) -> None:
        raise NotImplementedError

    @abstractmethod
    def download_file(self, remote_path: Path, local_path: Path) -> Path:
        raise NotImplementedError

    @abstractmethod
    def check_connectivity(self, host: HostDefinition) -> bool:
        raise NotImplementedError

