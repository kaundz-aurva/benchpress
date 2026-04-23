from __future__ import annotations

from typing import Any

from adapters.database.dto import SnapshotRequest
from adapters.database.service import DatabaseAdapter
from agents.sqlserver.client import SqlServerAgentClient
from agents.sqlserver.dto import ArtifactInfo
from orchestration.models import AuditMode, AuditProfile, RunArtifact


class SqlServerAgentDatabaseAdapter(DatabaseAdapter):
    def __init__(self, client: SqlServerAgentClient) -> None:
        self.client = client

    def validate_connectivity(self) -> bool:
        details = self.client.validate_connectivity()
        return bool(details.get("connected", True))

    def enable_audit(self, audit_profile: AuditProfile) -> None:
        if audit_profile.mode is not AuditMode.AUDIT_ON:
            raise ValueError("enable_audit requires an audit_on profile")
        self.client.enable_audit()

    def disable_audit(self, audit_profile: AuditProfile) -> None:
        if audit_profile.mode is not AuditMode.AUDIT_OFF:
            raise ValueError("disable_audit requires an audit_off profile")
        self.client.disable_audit()

    def run_sanity_checks(self) -> dict[str, Any]:
        return self.client.run_sanity_checks()

    def capture_pre_snapshot(self, request: SnapshotRequest) -> list[RunArtifact]:
        return self._capture_snapshot("pre", request)

    def capture_post_snapshot(self, request: SnapshotRequest) -> list[RunArtifact]:
        return self._capture_snapshot("post", request)

    def collect_database_metadata(self) -> dict[str, Any]:
        return self.client.collect_database_metadata()

    def _capture_snapshot(self, label: str, request: SnapshotRequest) -> list[RunArtifact]:
        artifacts = self.client.capture_snapshot(label, request.run_id)
        return [self._download_artifact(artifact, request) for artifact in artifacts]

    def _download_artifact(self, artifact: ArtifactInfo, request: SnapshotRequest) -> RunArtifact:
        local_path = self.client.download_artifact(artifact, request.output_dir)
        return RunArtifact(
            run_id=request.run_id,
            artifact_type=artifact.artifact_type,
            path=local_path,
            description=artifact.description,
        )

