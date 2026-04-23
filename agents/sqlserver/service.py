from __future__ import annotations

import platform
import shutil
import subprocess
from pathlib import Path
from typing import Callable, Sequence

from agents.sqlserver.models import AgentArtifact, LocalCommandResult, SqlServerAgentConfig


CommandRunner = Callable[[Sequence[str], int], LocalCommandResult]
PUBLIC_OUTPUT_LIMIT = 8192


class AgentCommandError(RuntimeError):
    def __init__(self, public_message: str) -> None:
        super().__init__(public_message)


class SqlServerAgentService:
    DEFAULT_SNAPSHOT_SQL = "SELECT name, database_id, create_date FROM sys.databases"
    DEFAULT_SANITY_SQL = "SELECT @@VERSION AS sqlserver_version"
    DEFAULT_METADATA_SQL = (
        "SELECT SERVERPROPERTY('ProductVersion') AS product_version, "
        "SERVERPROPERTY('Edition') AS edition"
    )

    def __init__(
        self,
        config: SqlServerAgentConfig,
        command_runner: CommandRunner | None = None,
        artifact_store: "AgentArtifactStore | None" = None,
        sql_provider: "SqlServerCommandProvider | None" = None,
        host_provider: "WindowsHostProvider | None" = None,
    ) -> None:
        self.config = config
        self.command_runner = command_runner or self._run_command
        self.config.staging_root.mkdir(parents=True, exist_ok=True)
        self.artifacts = artifact_store or AgentArtifactStore()
        self.sql = sql_provider or SqlServerCommandProvider(config, self.command_runner)
        self.host = host_provider or WindowsHostProvider(config, self.command_runner, self.artifacts)

    def health(self) -> dict[str, str]:
        return {"service": "benchpress-sqlserver-agent"}

    def enable_audit(self) -> dict[str, str]:
        if not self.config.enable_audit_sql and self.config.enable_audit_sql_file is None:
            raise NotImplementedError("enable_audit_sql is not configured")
        self._execute_sql_source(self.config.enable_audit_sql, self.config.enable_audit_sql_file)
        return {"audit": "enabled"}

    def disable_audit(self) -> dict[str, str]:
        if not self.config.disable_audit_sql and self.config.disable_audit_sql_file is None:
            raise NotImplementedError("disable_audit_sql is not configured")
        self._execute_sql_source(self.config.disable_audit_sql, self.config.disable_audit_sql_file)
        return {"audit": "disabled"}

    def validate_connectivity(self) -> dict[str, bool]:
        self.sql.execute_sql("SELECT 1")
        return {"connected": True}

    def run_sanity_checks(self) -> dict[str, object]:
        self._execute_sql_source(
            self.config.sanity_check_sql or self.DEFAULT_SANITY_SQL,
            self.config.sanity_check_sql_file,
        )
        return {"ok": True}

    def capture_snapshot(self, run_id: int, label: str) -> list[AgentArtifact]:
        if label not in {"pre", "post"}:
            raise ValueError("snapshot label must be 'pre' or 'post'")
        sql = self.config.pre_snapshot_sql if label == "pre" else self.config.post_snapshot_sql
        sql_file = (
            self.config.pre_snapshot_sql_file
            if label == "pre"
            else self.config.post_snapshot_sql_file
        )
        query = sql or self.DEFAULT_SNAPSHOT_SQL
        run_dir = self._run_dir(run_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = run_dir / f"sqlserver_{label}_snapshot.txt"
        self._execute_snapshot_source(query, sql_file, artifact_path)
        return [
            self.artifacts.register(
                artifact_type=f"database_{label}_snapshot",
                path=artifact_path,
                description=f"SQL Server {label}-run snapshot",
            )
        ]

    def start_metrics_collection(self, run_id: int) -> dict[str, str]:
        if not self.config.metrics_start_command:
            raise NotImplementedError("metrics_start_command is not configured")
        self.host.start_metrics()
        return {"metrics": "started", "run_id": str(run_id)}

    def stop_metrics_collection(self, run_id: int) -> list[AgentArtifact]:
        if not self.config.metrics_stop_command:
            raise NotImplementedError("metrics_stop_command is not configured")
        return self.host.stop_metrics(run_id)

    def collect_database_metadata(self) -> dict[str, object]:
        result = self._execute_sql_source(
            self.config.database_metadata_sql or self.DEFAULT_METADATA_SQL,
            self.config.database_metadata_sql_file,
        )
        return {
            "sql_connection_name": self.config.sql_connection_name,
            "result_text": _public_output(result.stdout),
            "truncated": len(result.stdout) > PUBLIC_OUTPUT_LIMIT,
        }

    def collect_filesystem_stats(self) -> dict[str, object]:
        return self.host.collect_filesystem_stats()

    def collect_host_metadata(self) -> dict[str, object]:
        return self.host.collect_host_metadata()

    def list_artifacts(self) -> list[AgentArtifact]:
        return self.artifacts.list()

    def get_artifact(self, artifact_id: int) -> AgentArtifact | None:
        return self.artifacts.get(artifact_id)

    def _run_command(self, command: Sequence[str], timeout_seconds: int) -> LocalCommandResult:
        try:
            completed = subprocess.run(
                list(command),
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
                shell=False,
                check=False,
            )
            return LocalCommandResult(
                command=_command_text(command),
                exit_code=completed.returncode,
                stdout=completed.stdout,
                stderr=completed.stderr,
            )
        except subprocess.TimeoutExpired:
            return LocalCommandResult(
                command=_command_text(command),
                exit_code=124,
                stderr="command timed out",
                timed_out=True,
            )

    def _run_dir(self, run_id: int) -> Path:
        if run_id <= 0:
            raise ValueError("run_id must be positive")
        return self.config.staging_root / f"run_{run_id}"

    def _execute_sql_source(
        self,
        sql: str | None,
        sql_file: Path | None,
    ) -> LocalCommandResult:
        if sql_file is not None:
            return self.sql.execute_sql_file(sql_file)
        if sql is None:
            raise NotImplementedError("SQL source is not configured")
        return self.sql.execute_sql(sql)

    def _execute_snapshot_source(
        self,
        sql: str | None,
        sql_file: Path | None,
        output_path: Path,
    ) -> LocalCommandResult:
        if sql_file is not None:
            return self.sql.execute_sql_file_to_file(sql_file, output_path)
        if sql is None:
            raise NotImplementedError("snapshot SQL source is not configured")
        return self.sql.execute_sql_to_file(sql, output_path)


class SqlServerCommandProvider:
    def __init__(self, config: SqlServerAgentConfig, command_runner: CommandRunner) -> None:
        self.config = config
        self.command_runner = command_runner

    def execute_sql(self, sql: str) -> LocalCommandResult:
        return self._execute_sql_args(
            [self.config.sqlcmd_path, "-S", self.config.sql_connection_name, "-Q", sql]
        )

    def execute_sql_to_file(self, sql: str, output_path: Path) -> LocalCommandResult:
        return self._execute_sql_args(
            [
                self.config.sqlcmd_path,
                "-S",
                self.config.sql_connection_name,
                "-Q",
                sql,
                "-o",
                str(output_path),
            ]
        )

    def execute_sql_file(self, input_path: Path) -> LocalCommandResult:
        return self._execute_sql_args(
            [
                self.config.sqlcmd_path,
                "-S",
                self.config.sql_connection_name,
                "-i",
                str(input_path),
            ]
        )

    def execute_sql_file_to_file(self, input_path: Path, output_path: Path) -> LocalCommandResult:
        return self._execute_sql_args(
            [
                self.config.sqlcmd_path,
                "-S",
                self.config.sql_connection_name,
                "-i",
                str(input_path),
                "-o",
                str(output_path),
            ]
        )

    def _execute_sql_args(self, args: Sequence[str]) -> LocalCommandResult:
        result = self.command_runner(args, self.config.command_timeout_seconds)
        if not result.succeeded:
            raise AgentCommandError("SQL command failed")
        return result


class WindowsHostProvider:
    def __init__(
        self,
        config: SqlServerAgentConfig,
        command_runner: CommandRunner,
        artifacts: "AgentArtifactStore",
    ) -> None:
        self.config = config
        self.command_runner = command_runner
        self.artifacts = artifacts

    def start_metrics(self) -> None:
        if self.config.metrics_start_command is None:
            raise NotImplementedError("metrics_start_command is not configured")
        self._execute_command(self.config.metrics_start_command)

    def stop_metrics(self, run_id: int) -> list[AgentArtifact]:
        if self.config.metrics_stop_command is None:
            raise NotImplementedError("metrics_stop_command is not configured")
        result = self._execute_command(self.config.metrics_stop_command)
        run_dir = self._run_dir(run_id)
        run_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = run_dir / "windows_metrics_stop.txt"
        artifact_path.write_text(result.stdout, encoding="utf-8")
        artifacts = [
            self.artifacts.register(
                artifact_type="host_metrics",
                path=artifact_path,
                description="Windows metrics collection output",
            )
        ]
        artifacts.extend(self._artifacts_from_stdout(result.stdout))
        return artifacts

    def collect_filesystem_stats(self) -> dict[str, object]:
        command_completed = False
        if self.config.filesystem_stats_command:
            self._execute_command(self.config.filesystem_stats_command)
            command_completed = True
        usage = shutil.disk_usage(self.config.staging_root)
        return {
            "staging_root": str(self.config.staging_root),
            "total_bytes": usage.total,
            "used_bytes": usage.used,
            "free_bytes": usage.free,
            "filesystem_stats_command_completed": command_completed,
        }

    def collect_host_metadata(self) -> dict[str, object]:
        metadata: dict[str, object] = {
            "system": platform.system(),
            "release": platform.release(),
            "version": platform.version(),
            "machine": platform.machine(),
            "node": platform.node(),
        }
        if self.config.host_metadata_command:
            self._execute_command(self.config.host_metadata_command)
            metadata["host_metadata_command_completed"] = True
        return metadata

    def _execute_command(self, command: Sequence[str]) -> LocalCommandResult:
        result = self.command_runner(command, self.config.command_timeout_seconds)
        if not result.succeeded:
            raise AgentCommandError("host command failed")
        return result

    def _run_dir(self, run_id: int) -> Path:
        if run_id <= 0:
            raise ValueError("run_id must be positive")
        return self.config.staging_root / f"run_{run_id}"

    def _artifacts_from_stdout(self, stdout: str) -> list[AgentArtifact]:
        artifacts: list[AgentArtifact] = []
        for line in stdout.splitlines():
            if not line.startswith("BENCHPRESS_ARTIFACT="):
                continue
            payload = line.removeprefix("BENCHPRESS_ARTIFACT=")
            parts = payload.split("|", 2)
            if len(parts) != 3:
                continue
            path_text, artifact_type, description = parts
            path = Path(path_text)
            if not path.exists():
                continue
            artifacts.append(
                self.artifacts.register(
                    artifact_type=artifact_type,
                    path=path,
                    description=description,
                )
            )
        return artifacts


class AgentArtifactStore:
    def __init__(self) -> None:
        self._artifact_counter = 0
        self._artifacts: dict[int, AgentArtifact] = {}

    def register(
        self,
        artifact_type: str,
        path: Path,
        description: str = "",
    ) -> AgentArtifact:
        self._artifact_counter += 1
        artifact = AgentArtifact(
            artifact_id=self._artifact_counter,
            artifact_type=artifact_type,
            path=path,
            description=description,
        )
        self._artifacts[artifact.artifact_id] = artifact
        return artifact

    def list(self) -> list[AgentArtifact]:
        return [self._artifacts[key] for key in sorted(self._artifacts)]

    def get(self, artifact_id: int) -> AgentArtifact | None:
        return self._artifacts.get(artifact_id)


def _command_text(command: Sequence[str]) -> str:
    return " ".join(command)


def _public_output(value: str) -> str:
    return value[:PUBLIC_OUTPUT_LIMIT]
