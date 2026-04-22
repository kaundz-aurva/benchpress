from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from adapters.database.dto import SnapshotRequest
from orchestration.models import AuditProfile, RunArtifact


class DatabaseAdapter(ABC):
    @abstractmethod
    def validate_connectivity(self) -> bool:
        raise NotImplementedError

    @abstractmethod
    def enable_audit(self, audit_profile: AuditProfile) -> None:
        raise NotImplementedError

    @abstractmethod
    def disable_audit(self, audit_profile: AuditProfile) -> None:
        raise NotImplementedError

    @abstractmethod
    def run_sanity_checks(self) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    def capture_pre_snapshot(self, request: SnapshotRequest) -> list[RunArtifact]:
        raise NotImplementedError

    @abstractmethod
    def capture_post_snapshot(self, request: SnapshotRequest) -> list[RunArtifact]:
        raise NotImplementedError

    @abstractmethod
    def collect_database_metadata(self) -> dict[str, Any]:
        raise NotImplementedError

