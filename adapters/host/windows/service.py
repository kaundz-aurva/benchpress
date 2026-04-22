from __future__ import annotations

from pathlib import Path
from typing import Any

from adapters.host.constants import OS_WINDOWS
from adapters.host.service import HostAdapter
from adapters.transport.dto import RemoteCommandRequest
from adapters.transport.service import TransportAdapter
from orchestration.models import HostDefinition, RunArtifact


class WindowsHostAdapter(HostAdapter):
    def __init__(
        self,
        host: HostDefinition,
        transport: TransportAdapter | None = None,
        metrics_start_command: str | None = None,
        metrics_stop_command: str | None = None,
        filesystem_stats_command: str | None = None,
    ) -> None:
        if host.os_type.strip().lower() != OS_WINDOWS:
            raise ValueError("WindowsHostAdapter requires os_type 'windows'")
        self.host = host
        self.transport = transport
        self.metrics_start_command = metrics_start_command
        self.metrics_stop_command = metrics_stop_command
        self.filesystem_stats_command = filesystem_stats_command

    def _execute(self, command: str, timeout_seconds: int = 120) -> str:
        if self.transport is None:
            raise NotImplementedError("Windows host commands require a transport adapter")
        result = self.transport.execute_command(
            RemoteCommandRequest(
                host=self.host,
                command=command,
                timeout_seconds=timeout_seconds,
            )
        )
        if not result.succeeded:
            raise RuntimeError(f"Windows host command failed: {result.stderr or result.stdout}")
        return result.stdout

    def start_metrics_collection(self, run_id: int, output_dir: Path) -> None:
        if not self.metrics_start_command:
            raise NotImplementedError("metrics_start_command is not configured")
        self._execute(self.metrics_start_command)

    def stop_metrics_collection(self, run_id: int, output_dir: Path) -> list[RunArtifact]:
        if not self.metrics_stop_command:
            raise NotImplementedError("metrics_stop_command is not configured")
        output_dir.mkdir(parents=True, exist_ok=True)
        stdout = self._execute(self.metrics_stop_command)
        artifact_path = output_dir / "windows_metrics_stop.txt"
        artifact_path.write_text(stdout, encoding="utf-8")
        return [
            RunArtifact(
                run_id=run_id,
                artifact_type="host_metrics",
                path=artifact_path,
                description="Windows metrics collection output",
            )
        ]

    def collect_filesystem_stats(self) -> dict[str, Any]:
        if not self.filesystem_stats_command:
            raise NotImplementedError("filesystem_stats_command is not configured")
        return {"raw": self._execute(self.filesystem_stats_command)}

    def collect_host_metadata(self) -> dict[str, Any]:
        return {
            "host": self.host.hostname,
            "os_type": self.host.os_type,
            "vcpus": self.host.vcpus,
            "memory_gb": self.host.memory_gb,
        }

