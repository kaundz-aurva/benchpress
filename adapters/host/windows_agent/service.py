from __future__ import annotations

from pathlib import Path
from typing import Any

from adapters.host.service import HostAdapter
from agents.sqlserver.client import SqlServerAgentClient
from agents.sqlserver.dto import ArtifactInfo
from orchestration.models import RunArtifact


class WindowsAgentHostAdapter(HostAdapter):
    def __init__(self, client: SqlServerAgentClient) -> None:
        self.client = client

    def start_metrics_collection(self, run_id: int, output_dir: Path) -> None:
        self.client.start_metrics_collection(run_id)

    def stop_metrics_collection(self, run_id: int, output_dir: Path) -> list[RunArtifact]:
        artifacts = self.client.stop_metrics_collection(run_id)
        downloaded = [self._download_artifact(run_id, artifact, output_dir) for artifact in artifacts]
        if any(artifact.artifact_type == "host_metrics_csv" for artifact in downloaded):
            return downloaded
        raise RuntimeError("Metrics stop did not produce a host_metrics_csv artifact.")

    def collect_filesystem_stats(self) -> dict[str, Any]:
        return self.client.collect_filesystem_stats()

    def collect_host_metadata(self) -> dict[str, Any]:
        return self.client.collect_host_metadata()

    def _download_artifact(
        self,
        run_id: int,
        artifact: ArtifactInfo,
        output_dir: Path,
    ) -> RunArtifact:
        local_path = self.client.download_artifact(artifact, output_dir)
        return RunArtifact(
            run_id=run_id,
            artifact_type=artifact.artifact_type,
            path=local_path,
            description=artifact.description,
        )
