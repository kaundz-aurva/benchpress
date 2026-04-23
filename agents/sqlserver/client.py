from __future__ import annotations

from pathlib import Path
from typing import Any

import httpx

from agents.sqlserver.dto import ArtifactInfo


class SqlServerAgentClient:
    def __init__(
        self,
        base_url: str,
        bearer_token: str,
        timeout_seconds: float = 120.0,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        if not base_url.strip():
            raise ValueError("base_url is required")
        if not bearer_token.strip():
            raise ValueError("bearer_token is required")
        if timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")
        self.client = httpx.Client(
            base_url=base_url.rstrip("/"),
            headers={"Authorization": f"Bearer {bearer_token}"},
            timeout=timeout_seconds,
            transport=transport,
        )

    def close(self) -> None:
        self.client.close()

    def health(self) -> dict[str, Any]:
        return self._request("GET", "/health", authenticated=False)

    def enable_audit(self) -> dict[str, Any]:
        return self._request("POST", "/audit/enable")["details"]

    def disable_audit(self) -> dict[str, Any]:
        return self._request("POST", "/audit/disable")["details"]

    def validate_connectivity(self) -> dict[str, Any]:
        return self._request("POST", "/database/connectivity")["details"]

    def run_sanity_checks(self) -> dict[str, Any]:
        return self._request("POST", "/database/sanity")["metadata"]

    def capture_snapshot(self, label: str, run_id: int) -> list[ArtifactInfo]:
        return self._artifact_list(
            self._request("POST", f"/snapshots/{label}", json={"run_id": run_id})
        )

    def start_metrics_collection(self, run_id: int) -> dict[str, Any]:
        return self._request("POST", "/metrics/start", json={"run_id": run_id})["details"]

    def stop_metrics_collection(self, run_id: int) -> list[ArtifactInfo]:
        return self._artifact_list(
            self._request("POST", "/metrics/stop", json={"run_id": run_id})
        )

    def collect_database_metadata(self) -> dict[str, Any]:
        return self._request("GET", "/metadata/database")["metadata"]

    def collect_filesystem_stats(self) -> dict[str, Any]:
        return self._request("GET", "/metadata/filesystem")["metadata"]

    def collect_host_metadata(self) -> dict[str, Any]:
        return self._request("GET", "/metadata/host")["metadata"]

    def list_artifacts(self) -> list[ArtifactInfo]:
        return self._artifact_list(self._request("GET", "/artifacts"))

    def download_artifact(self, artifact: ArtifactInfo, destination_dir: Path) -> Path:
        destination_dir.mkdir(parents=True, exist_ok=True)
        destination_path = destination_dir / Path(artifact.path).name
        with self.client.stream("GET", f"/artifacts/{artifact.artifact_id}") as response:
            self._raise_for_status(response)
            with destination_path.open("wb") as destination:
                for chunk in response.iter_bytes():
                    destination.write(chunk)
        return destination_path

    def _request(
        self,
        method: str,
        path: str,
        json: dict[str, Any] | None = None,
        authenticated: bool = True,
    ) -> dict[str, Any]:
        headers = None
        if not authenticated:
            headers = {"Authorization": ""}
        response = self.client.request(method, path, json=json, headers=headers)
        self._raise_for_status(response)
        data = response.json()
        if not isinstance(data, dict):
            raise RuntimeError(f"agent returned non-object response for {path}")
        return data

    def _raise_for_status(self, response: httpx.Response) -> None:
        if response.status_code < 400:
            return
        try:
            detail = response.json().get("detail", response.text)
        except ValueError:
            detail = response.text
        raise RuntimeError(f"SQL Server agent request failed: {response.status_code} {detail}")

    def _artifact_list(self, data: dict[str, Any]) -> list[ArtifactInfo]:
        artifacts = data.get("artifacts", [])
        if not isinstance(artifacts, list):
            raise RuntimeError("agent artifact response is invalid")
        return [ArtifactInfo.model_validate(artifact) for artifact in artifacts]
