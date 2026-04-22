from __future__ import annotations

from subprocess import list2cmdline
from typing import Any

from adapters.database.constants import DATABASE_ENGINE_SQLSERVER, SQLSERVER_VERSION_2019
from adapters.database.dto import SnapshotRequest
from adapters.database.service import DatabaseAdapter
from adapters.transport.dto import RemoteCommandRequest, RemoteCommandResult
from adapters.transport.service import TransportAdapter
from orchestration.models import AuditMode, AuditProfile, HostDefinition, RunArtifact


class SqlServerDatabaseAdapter(DatabaseAdapter):
    def __init__(
        self,
        host: HostDefinition,
        connection_name: str,
        transport: TransportAdapter,
        enable_audit_script: str | None = None,
        disable_audit_script: str | None = None,
        snapshot_query: str | None = None,
        sqlcmd_path: str = "sqlcmd",
    ) -> None:
        if not connection_name.strip():
            raise ValueError("connection_name is required")
        if not sqlcmd_path.strip():
            raise ValueError("sqlcmd_path is required")
        if transport is None:
            raise ValueError("transport is required")
        self.host = host
        self.connection_name = connection_name
        self.transport = transport
        self.enable_audit_script = enable_audit_script
        self.disable_audit_script = disable_audit_script
        self.snapshot_query = snapshot_query
        self.sqlcmd_path = sqlcmd_path

    def _execute_sql(self, sql: str, timeout_seconds: int = 120) -> RemoteCommandResult:
        command = self._build_sqlcmd_command(sql)
        result = self._execute_command(command, timeout_seconds)
        if not result.succeeded:
            raise RuntimeError(f"SQL Server command failed: {result.stderr or result.stdout}")
        return result

    def _execute_sql_to_file(
        self,
        sql: str,
        output_path: str,
        timeout_seconds: int = 120,
    ) -> RemoteCommandResult:
        command = self._build_sqlcmd_command(sql, output_path=output_path)
        result = self._execute_command(command, timeout_seconds)
        if not result.succeeded:
            raise RuntimeError(f"SQL Server command failed: {result.stderr or result.stdout}")
        return result

    def _build_sqlcmd_command(self, sql: str, output_path: str | None = None) -> str:
        args = [
            self.sqlcmd_path,
            "-S",
            self.connection_name,
            "-Q",
            sql,
        ]
        if output_path is not None:
            args.extend(["-o", output_path])
        return list2cmdline(args)

    def _execute_command(self, command: str, timeout_seconds: int) -> RemoteCommandResult:
        return self.transport.execute_command(
            RemoteCommandRequest(
                host=self.host,
                command=command,
                timeout_seconds=timeout_seconds,
            )
        )

    def validate_connectivity(self) -> bool:
        self._execute_sql("SELECT 1")
        return True

    def enable_audit(self, audit_profile: AuditProfile) -> None:
        if audit_profile.mode is not AuditMode.AUDIT_ON:
            raise ValueError("enable_audit requires an audit_on profile")
        if not self.enable_audit_script:
            raise NotImplementedError("enable_audit_script is not configured")
        self._execute_sql(self.enable_audit_script)

    def disable_audit(self, audit_profile: AuditProfile) -> None:
        if audit_profile.mode is not AuditMode.AUDIT_OFF:
            raise ValueError("disable_audit requires an audit_off profile")
        if not self.disable_audit_script:
            raise NotImplementedError("disable_audit_script is not configured")
        self._execute_sql(self.disable_audit_script)

    def run_sanity_checks(self) -> dict[str, Any]:
        return {
            "database_engine": DATABASE_ENGINE_SQLSERVER,
            "database_version": SQLSERVER_VERSION_2019,
            "connection_name": self.connection_name,
        }

    def capture_pre_snapshot(self, request: SnapshotRequest) -> list[RunArtifact]:
        return self._capture_snapshot(request, "pre")

    def capture_post_snapshot(self, request: SnapshotRequest) -> list[RunArtifact]:
        return self._capture_snapshot(request, "post")

    def collect_database_metadata(self) -> dict[str, Any]:
        return {
            "database_engine": DATABASE_ENGINE_SQLSERVER,
            "database_version": SQLSERVER_VERSION_2019,
            "host": self.host.hostname,
        }

    def _capture_snapshot(self, request: SnapshotRequest, prefix: str) -> list[RunArtifact]:
        if not self.snapshot_query:
            raise NotImplementedError("snapshot_query is not configured")
        request.output_dir.mkdir(parents=True, exist_ok=True)
        artifact_path = request.output_dir / f"sqlserver_{prefix}_snapshot.txt"
        self._execute_sql_to_file(self.snapshot_query, str(artifact_path))
        return [
            RunArtifact(
                run_id=request.run_id,
                artifact_type=f"database_{prefix}_snapshot",
                path=artifact_path,
                description=f"SQL Server {prefix}-run snapshot",
            )
        ]
