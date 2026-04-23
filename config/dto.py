from __future__ import annotations

from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class BenchmarkProfileSpecDto(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    database_engine: str = "sqlserver"
    database_version: str = "2019"
    cloud_provider: str = "gcp"
    description: str = ""


class HostSpecDto(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    role: str
    os_type: str
    hostname: str
    vcpus: int = Field(gt=0)
    memory_gb: int = Field(gt=0)
    cloud_instance_id: str = ""


class AgentSpecDto(BaseModel):
    model_config = ConfigDict(extra="forbid")

    base_url: str
    bearer_token_env: str = "BENCHPRESS_AGENT_TOKEN"
    timeout_seconds: float = Field(default=120.0, gt=0)


class TimingSpecDto(BaseModel):
    model_config = ConfigDict(extra="forbid")

    warmup_minutes: int = Field(default=10, ge=0)
    measured_minutes: int = Field(default=20, gt=0)
    cooldown_minutes: int = Field(default=5, ge=0)


class WorkloadSpecDto(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tool: str = "hammerdb"
    hammerdb_executable_path: Path
    hammerdb_script_path: Path
    result_filename: str = "hammerdb_stdout.txt"
    virtual_user_ladder: list[int] = Field(default_factory=lambda: [10, 20, 40, 60])
    timings: TimingSpecDto = Field(default_factory=TimingSpecDto)
    repetitions: int = Field(default=3, gt=0)

    @field_validator("virtual_user_ladder")
    @classmethod
    def _validate_vu_ladder(cls, value: list[int]) -> list[int]:
        if not value:
            raise ValueError("virtual_user_ladder must not be empty")
        if any(isinstance(item, bool) or item <= 0 for item in value):
            raise ValueError("virtual_user_ladder values must be positive integers")
        return value


class AuditSpecDto(BaseModel):
    model_config = ConfigDict(extra="forbid")

    modes: list[str] = Field(default_factory=lambda: ["audit_off", "audit_on"])

    @field_validator("modes")
    @classmethod
    def _validate_modes(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("modes must not be empty")
        return value


class SqlServerAuditScriptSpecDto(BaseModel):
    model_config = ConfigDict(extra="forbid")

    audit_name: str = "Audit-benchpress"
    server_audit_spec_name: str = "Server-Audit-Spec-benchpress"
    database_audit_spec_name_template: str = "Db-Audit-Spec-benchpress-{database}"
    audit_file_path: str
    max_size_mb: int = Field(default=10, gt=0)
    queue_delay_ms: int = Field(default=1000, ge=0)
    selected_databases: list[str]
    excluded_principals: list[str] = Field(
        default_factory=lambda: [
            "rdsa",
            "sa",
            "NT AUTHORITY\\SYSTEM",
            "NT AUTHORITY\\LOCAL SERVICE",
        ]
    )
    excluded_client_ips: list[str] = Field(default_factory=lambda: ["local machine"])
    excluded_database_names: list[str] = Field(default_factory=lambda: ["msdb"])
    excluded_schema_names: list[str] = Field(default_factory=lambda: ["sys", "INFORMATION_SCHEMA"])
    server_audit_groups: list[str] = Field(
        default_factory=lambda: [
            "SUCCESSFUL_LOGIN_GROUP",
            "FAILED_LOGIN_GROUP",
            "LOGOUT_GROUP",
            "SUCCESSFUL_DATABASE_AUTHENTICATION_GROUP",
            "FAILED_DATABASE_AUTHENTICATION_GROUP",
            "DATABASE_LOGOUT_GROUP",
            "APPLICATION_ROLE_CHANGE_PASSWORD_GROUP",
            "SERVER_OBJECT_CHANGE_GROUP",
            "SERVER_PERMISSION_CHANGE_GROUP",
            "SERVER_PRINCIPAL_CHANGE_GROUP",
            "SERVER_ROLE_MEMBER_CHANGE_GROUP",
            "SERVER_PRINCIPAL_IMPERSONATION_GROUP",
            "DATABASE_CHANGE_GROUP",
            "SERVER_OBJECT_OWNERSHIP_CHANGE_GROUP",
            "SERVER_OBJECT_PERMISSION_CHANGE_GROUP",
            "SERVER_OPERATION_GROUP",
            "SERVER_STATE_CHANGE_GROUP",
            "USER_CHANGE_PASSWORD_GROUP",
            "DATABASE_OBJECT_CHANGE_GROUP",
            "DATABASE_OBJECT_OWNERSHIP_CHANGE_GROUP",
            "DATABASE_OBJECT_PERMISSION_CHANGE_GROUP",
            "DATABASE_OWNERSHIP_CHANGE_GROUP",
            "DATABASE_PERMISSION_CHANGE_GROUP",
            "DATABASE_PRINCIPAL_CHANGE_GROUP",
            "DATABASE_ROLE_MEMBER_CHANGE_GROUP",
            "SCHEMA_OBJECT_CHANGE_GROUP",
            "TRANSACTION_GROUP",
            "SCHEMA_OBJECT_PERMISSION_CHANGE_GROUP",
        ]
    )
    database_audit_groups: list[str] = Field(
        default_factory=lambda: [
            "DATABASE_OBJECT_CHANGE_GROUP",
            "DATABASE_PERMISSION_CHANGE_GROUP",
            "SCHEMA_OBJECT_CHANGE_GROUP",
            "SCHEMA_OBJECT_PERMISSION_CHANGE_GROUP",
        ]
    )
    database_statement_actions: list[str] = Field(
        default_factory=lambda: ["INSERT", "UPDATE", "DELETE", "EXECUTE", "SELECT"]
    )

    @field_validator(
        "audit_name",
        "server_audit_spec_name",
        "database_audit_spec_name_template",
        "audit_file_path",
    )
    @classmethod
    def _validate_required_text(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("value is required")
        return value

    @field_validator("selected_databases", "server_audit_groups", "database_audit_groups")
    @classmethod
    def _validate_non_empty_text_list(cls, value: list[str]) -> list[str]:
        if not value or any(not item.strip() for item in value):
            raise ValueError("list must contain non-empty strings")
        return value

    @field_validator("database_audit_spec_name_template")
    @classmethod
    def _validate_db_template(cls, value: str) -> str:
        if "{database}" not in value:
            raise ValueError("database_audit_spec_name_template must include {database}")
        return value


class HammerDbTprocCSpecDto(BaseModel):
    model_config = ConfigDict(extra="forbid")

    workload: Literal["tprocc"] = "tprocc"
    sql_server: str
    sql_port: int = Field(default=1433, gt=0)
    database_name: str
    username_env: str = "BENCHPRESS_SQL_USER"
    password_env: str = "BENCHPRESS_SQL_PASSWORD"
    warehouses: int = Field(default=100, gt=0)
    build_schema: bool = False
    driver_mode: Literal["timed", "test"] = "timed"

    @field_validator("sql_server", "database_name", "username_env", "password_env")
    @classmethod
    def _validate_required_text(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("value is required")
        return value


class WindowsMetricsScriptSpecDto(BaseModel):
    model_config = ConfigDict(extra="forbid")

    collector_name: str = "BenchpressSqlMetrics"
    sample_interval_seconds: int = Field(default=5, gt=0)
    output_root: Path = Path("C:/benchpress/agent_artifacts/metrics")
    max_size_mb: int = Field(default=1024, gt=0)
    counters: list[str] = Field(
        default_factory=lambda: [
            "\\Processor(_Total)\\% Processor Time",
            "\\Memory\\Available MBytes",
            "\\LogicalDisk(_Total)\\Avg. Disk sec/Read",
            "\\LogicalDisk(_Total)\\Avg. Disk sec/Write",
            "\\LogicalDisk(_Total)\\Disk Reads/sec",
            "\\LogicalDisk(_Total)\\Disk Writes/sec",
            "\\Network Interface(*)\\Bytes Total/sec",
            "\\Process(sqlservr)\\% Processor Time",
            "\\Process(sqlservr)\\Working Set",
        ]
    )

    @field_validator("collector_name")
    @classmethod
    def _validate_collector_name(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("collector_name is required")
        return value

    @field_validator("counters")
    @classmethod
    def _validate_counters(cls, value: list[str]) -> list[str]:
        if not value or any(not counter.strip() for counter in value):
            raise ValueError("counters must contain non-empty strings")
        return value


class AssetGenerationSpecDto(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sql_connection_name: str = "localhost"
    sqlcmd_path: str = "sqlcmd"
    staging_root: Path = Path("C:/benchpress/agent_artifacts")
    command_timeout_seconds: int = Field(default=120, gt=0)
    audit: SqlServerAuditScriptSpecDto
    hammerdb: HammerDbTprocCSpecDto
    metrics: WindowsMetricsScriptSpecDto = Field(default_factory=WindowsMetricsScriptSpecDto)

    @field_validator("sql_connection_name", "sqlcmd_path")
    @classmethod
    def _validate_required_text(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("value is required")
        return value


class StorageSpecDto(BaseModel):
    model_config = ConfigDict(extra="forbid")

    sqlite_path: Path = Path("benchpress.sqlite3")
    output_root: Path = Path("outputs")


class BenchmarkRuntimeSpecDto(BaseModel):
    model_config = ConfigDict(extra="forbid")

    benchmark_profile: BenchmarkProfileSpecDto
    target_host: HostSpecDto
    client_host: HostSpecDto
    agent: AgentSpecDto
    workload: WorkloadSpecDto
    audit: AuditSpecDto = Field(default_factory=AuditSpecDto)
    storage: StorageSpecDto = Field(default_factory=StorageSpecDto)
    assets: AssetGenerationSpecDto | None = None

    @field_validator("benchmark_profile", mode="after")
    @classmethod
    def _validate_sqlserver_first(cls, value: BenchmarkProfileSpecDto) -> BenchmarkProfileSpecDto:
        if value.database_engine.lower() != "sqlserver":
            raise ValueError("only sqlserver runtime specs are supported in this slice")
        return value
