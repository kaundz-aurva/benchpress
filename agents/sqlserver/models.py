from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Sequence


def _required_text(value: str, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} is required")


def _positive_int(value: int, field_name: str) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field_name} must be a positive integer")


@dataclass(frozen=True)
class AgentArtifact:
    artifact_id: int
    artifact_type: str
    path: Path
    description: str = ""

    def __post_init__(self) -> None:
        _positive_int(self.artifact_id, "artifact_id")
        _required_text(self.artifact_type, "artifact_type")
        object.__setattr__(self, "path", Path(self.path))


@dataclass(frozen=True)
class LocalCommandResult:
    command: str
    exit_code: int
    stdout: str = ""
    stderr: str = ""
    timed_out: bool = False

    @property
    def succeeded(self) -> bool:
        return self.exit_code == 0 and not self.timed_out


@dataclass(frozen=True)
class SqlServerAgentConfig:
    sql_connection_name: str
    staging_root: Path
    bearer_token_env: str = "BENCHPRESS_AGENT_TOKEN"
    sqlcmd_path: str = "sqlcmd"
    command_timeout_seconds: int = 120
    enable_audit_sql: str | None = None
    enable_audit_sql_file: Path | None = None
    disable_audit_sql: str | None = None
    disable_audit_sql_file: Path | None = None
    pre_snapshot_sql: str | None = None
    pre_snapshot_sql_file: Path | None = None
    post_snapshot_sql: str | None = None
    post_snapshot_sql_file: Path | None = None
    sanity_check_sql: str | None = None
    sanity_check_sql_file: Path | None = None
    database_metadata_sql: str | None = None
    database_metadata_sql_file: Path | None = None
    metrics_start_command: tuple[str, ...] | None = None
    metrics_stop_command: tuple[str, ...] | None = None
    filesystem_stats_command: tuple[str, ...] | None = None
    host_metadata_command: tuple[str, ...] | None = None

    def __post_init__(self) -> None:
        _required_text(self.sql_connection_name, "sql_connection_name")
        _required_text(self.sqlcmd_path, "sqlcmd_path")
        _required_text(self.bearer_token_env, "bearer_token_env")
        _positive_int(self.command_timeout_seconds, "command_timeout_seconds")
        object.__setattr__(self, "staging_root", Path(self.staging_root))
        for field_name in (
            "enable_audit_sql_file",
            "disable_audit_sql_file",
            "pre_snapshot_sql_file",
            "post_snapshot_sql_file",
            "sanity_check_sql_file",
            "database_metadata_sql_file",
        ):
            value = getattr(self, field_name)
            if value is not None:
                object.__setattr__(self, field_name, Path(value))

    @classmethod
    def from_json_file(cls, path: Path | str) -> "SqlServerAgentConfig":
        config_path = Path(path)
        data = json.loads(config_path.read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError("agent config must be a JSON object")
        return cls.from_mapping(data)

    @classmethod
    def from_mapping(cls, data: dict[str, Any]) -> "SqlServerAgentConfig":
        return cls(
            sql_connection_name=str(data["sql_connection_name"]),
            staging_root=Path(data.get("staging_root", "agent_artifacts")),
            bearer_token_env=str(data.get("bearer_token_env", "BENCHPRESS_AGENT_TOKEN")),
            sqlcmd_path=str(data.get("sqlcmd_path", "sqlcmd")),
            command_timeout_seconds=int(data.get("command_timeout_seconds", 120)),
            enable_audit_sql=_optional_text(data.get("enable_audit_sql")),
            enable_audit_sql_file=_optional_path(data.get("enable_audit_sql_file")),
            disable_audit_sql=_optional_text(data.get("disable_audit_sql")),
            disable_audit_sql_file=_optional_path(data.get("disable_audit_sql_file")),
            pre_snapshot_sql=_optional_text(data.get("pre_snapshot_sql")),
            pre_snapshot_sql_file=_optional_path(data.get("pre_snapshot_sql_file")),
            post_snapshot_sql=_optional_text(data.get("post_snapshot_sql")),
            post_snapshot_sql_file=_optional_path(data.get("post_snapshot_sql_file")),
            sanity_check_sql=_optional_text(data.get("sanity_check_sql")),
            sanity_check_sql_file=_optional_path(data.get("sanity_check_sql_file")),
            database_metadata_sql=_optional_text(data.get("database_metadata_sql")),
            database_metadata_sql_file=_optional_path(data.get("database_metadata_sql_file")),
            metrics_start_command=_optional_command(data.get("metrics_start_command")),
            metrics_stop_command=_optional_command(data.get("metrics_stop_command")),
            filesystem_stats_command=_optional_command(data.get("filesystem_stats_command")),
            host_metadata_command=_optional_command(data.get("host_metadata_command")),
        )

    def resolve_bearer_token(self) -> str:
        token = os.environ.get(self.bearer_token_env, "")
        _required_text(token, self.bearer_token_env)
        return token


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    if not text.strip():
        return None
    return text


def _optional_path(value: Any) -> Path | None:
    text = _optional_text(value)
    if text is None:
        return None
    return Path(text)


def _optional_command(value: Any) -> tuple[str, ...] | None:
    if value is None:
        return None
    if not isinstance(value, Sequence) or isinstance(value, str):
        raise ValueError("configured commands must be arrays of arguments")
    command = tuple(str(item) for item in value)
    if not command or any(not item.strip() for item in command):
        raise ValueError("configured command arguments must be non-empty")
    return command
