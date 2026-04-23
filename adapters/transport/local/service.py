from __future__ import annotations

import os
import shutil
import subprocess
import time
from pathlib import Path

from adapters.transport.dto import RemoteCommandRequest, RemoteCommandResult
from adapters.transport.service import TransportAdapter
from orchestration.models import HostDefinition


class LocalTransport(TransportAdapter):
    def execute_command(self, request: RemoteCommandRequest) -> RemoteCommandResult:
        started = time.monotonic()
        environment = os.environ.copy()
        environment.update(request.environment)
        try:
            completed = subprocess.run(
                request.command,
                cwd=request.working_dir,
                env=environment,
                shell=True,
                capture_output=True,
                text=True,
                timeout=request.timeout_seconds,
                check=False,
            )
            return RemoteCommandResult(
                command=request.command,
                exit_code=completed.returncode,
                stdout=completed.stdout,
                stderr=completed.stderr,
                duration_seconds=time.monotonic() - started,
            )
        except subprocess.TimeoutExpired as exc:
            return RemoteCommandResult(
                command=request.command,
                exit_code=124,
                stdout=exc.stdout or "",
                stderr=exc.stderr or "command timed out",
                duration_seconds=time.monotonic() - started,
                timed_out=True,
            )

    def upload_file(self, local_path: Path, remote_path: Path) -> None:
        remote_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(local_path, remote_path)

    def download_file(self, remote_path: Path, local_path: Path) -> Path:
        local_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(remote_path, local_path)
        return local_path

    def check_connectivity(self, host: HostDefinition) -> bool:
        return True

