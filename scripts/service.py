from __future__ import annotations

import json
from pathlib import Path

from config.dto import (
    AssetGenerationSpecDto,
    BenchmarkRuntimeSpecDto,
    HammerDbTprocCSpecDto,
    SqlServerAuditScriptSpecDto,
    WindowsMetricsScriptSpecDto,
)
from scripts.constants import (
    AUDIT_DISABLE_FILENAME,
    AUDIT_ENABLE_FILENAME,
    AUDIT_METADATA_FILENAME,
    AUDIT_POST_SNAPSHOT_FILENAME,
    AUDIT_PRE_SNAPSHOT_FILENAME,
    GENERATED_AGENT_CONFIG_FILENAME,
    HAMMERDB_TPROCC_FILENAME,
    METRICS_START_FILENAME,
    METRICS_STOP_FILENAME,
)
from scripts.models import GeneratedBenchmarkAssets


class BenchmarkAssetGenerationService:
    def generate(self, spec: BenchmarkRuntimeSpecDto, output_dir: Path | str) -> GeneratedBenchmarkAssets:
        if spec.assets is None:
            raise ValueError("spec.assets is required for asset generation")
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        assets = GeneratedBenchmarkAssets(
            output_dir=output_path,
            audit_enable_sql=output_path / AUDIT_ENABLE_FILENAME,
            audit_disable_sql=output_path / AUDIT_DISABLE_FILENAME,
            audit_pre_snapshot_sql=output_path / AUDIT_PRE_SNAPSHOT_FILENAME,
            audit_post_snapshot_sql=output_path / AUDIT_POST_SNAPSHOT_FILENAME,
            audit_metadata_sql=output_path / AUDIT_METADATA_FILENAME,
            hammerdb_tcl=output_path / HAMMERDB_TPROCC_FILENAME,
            metrics_start_ps1=output_path / METRICS_START_FILENAME,
            metrics_stop_ps1=output_path / METRICS_STOP_FILENAME,
            agent_config_json=output_path / GENERATED_AGENT_CONFIG_FILENAME,
        )

        assets.audit_enable_sql.write_text(
            SqlServerAuditScriptGenerator().render_enable(spec.assets.audit),
            encoding="utf-8",
        )
        assets.audit_disable_sql.write_text(
            SqlServerAuditScriptGenerator().render_disable(spec.assets.audit),
            encoding="utf-8",
        )
        assets.audit_pre_snapshot_sql.write_text(
            SqlServerAuditScriptGenerator().render_snapshot(spec.assets.audit, "pre"),
            encoding="utf-8",
        )
        assets.audit_post_snapshot_sql.write_text(
            SqlServerAuditScriptGenerator().render_snapshot(spec.assets.audit, "post"),
            encoding="utf-8",
        )
        assets.audit_metadata_sql.write_text(
            SqlServerAuditScriptGenerator().render_metadata(spec.assets.audit),
            encoding="utf-8",
        )
        assets.hammerdb_tcl.write_text(
            HammerDbTprocCScriptGenerator().render(spec.assets.hammerdb, spec),
            encoding="utf-8",
        )
        assets.metrics_start_ps1.write_text(
            WindowsLogmanMetricsScriptGenerator().render_start(spec.assets.metrics),
            encoding="utf-8",
        )
        assets.metrics_stop_ps1.write_text(
            WindowsLogmanMetricsScriptGenerator().render_stop(spec.assets.metrics),
            encoding="utf-8",
        )
        assets.agent_config_json.write_text(
            json.dumps(_agent_config(spec.assets, assets), indent=2, sort_keys=True),
            encoding="utf-8",
        )
        return assets


class SqlServerAuditScriptGenerator:
    def render_enable(self, spec: SqlServerAuditScriptSpecDto) -> str:
        lines: list[str] = [
            "USE [master]",
            "GO",
            "",
            "-- Recreate benchmark-owned database audit specifications.",
        ]
        for database in spec.selected_databases:
            lines.extend(self._drop_database_spec(spec, database))
        lines.extend(
            [
                "USE [master]",
                "GO",
                "",
                "-- Recreate benchmark-owned server audit specification.",
                f"IF EXISTS (SELECT 1 FROM sys.server_audit_specifications WHERE name = {_sql_string(spec.server_audit_spec_name)})",
                "BEGIN",
                f"    ALTER SERVER AUDIT SPECIFICATION {_bracket(spec.server_audit_spec_name)} WITH (STATE = OFF);",
                f"    DROP SERVER AUDIT SPECIFICATION {_bracket(spec.server_audit_spec_name)};",
                "END",
                "GO",
                "",
                "-- Recreate benchmark-owned server audit.",
                f"IF EXISTS (SELECT 1 FROM sys.server_audits WHERE name = {_sql_string(spec.audit_name)})",
                "BEGIN",
                f"    ALTER SERVER AUDIT {_bracket(spec.audit_name)} WITH (STATE = OFF);",
                f"    DROP SERVER AUDIT {_bracket(spec.audit_name)};",
                "END",
                "GO",
                "",
                f"CREATE SERVER AUDIT {_bracket(spec.audit_name)}",
                "TO FILE (",
                f"    FILEPATH = {_sql_string(spec.audit_file_path)},",
                f"    MAXSIZE = {spec.max_size_mb} MB,",
                "    RESERVE_DISK_SPACE = OFF",
                ")",
                "WITH (",
                f"    QUEUE_DELAY = {spec.queue_delay_ms},",
                "    ON_FAILURE = CONTINUE",
                ");",
                "GO",
            ]
        )
        filter_clause = self._filter_clause(spec)
        if filter_clause:
            lines.extend(
                [
                    f"ALTER SERVER AUDIT {_bracket(spec.audit_name)}",
                    "WHERE",
                    filter_clause,
                    "GO",
                ]
            )
        lines.extend(
            [
                f"ALTER SERVER AUDIT {_bracket(spec.audit_name)} WITH (STATE = ON);",
                "GO",
                "",
                f"CREATE SERVER AUDIT SPECIFICATION {_bracket(spec.server_audit_spec_name)}",
                f"FOR SERVER AUDIT {_bracket(spec.audit_name)}",
                self._audit_items(spec.server_audit_groups),
                "WITH (STATE = ON);",
                "GO",
                "",
            ]
        )
        for database in spec.selected_databases:
            lines.extend(self._create_database_spec(spec, database))
        return "\n".join(lines).rstrip() + "\n"

    def render_disable(self, spec: SqlServerAuditScriptSpecDto) -> str:
        lines: list[str] = ["-- Disable benchmark-owned audit objects without deleting audit files."]
        for database in spec.selected_databases:
            db_spec_name = self._database_spec_name(spec, database)
            lines.extend(
                [
                    f"USE {_bracket(database)}",
                    "GO",
                    f"IF EXISTS (SELECT 1 FROM sys.database_audit_specifications WHERE name = {_sql_string(db_spec_name)})",
                    "BEGIN",
                    f"    ALTER DATABASE AUDIT SPECIFICATION {_bracket(db_spec_name)} WITH (STATE = OFF);",
                    "END",
                    "GO",
                    "",
                ]
            )
        lines.extend(
            [
                "USE [master]",
                "GO",
                f"IF EXISTS (SELECT 1 FROM sys.server_audit_specifications WHERE name = {_sql_string(spec.server_audit_spec_name)})",
                "BEGIN",
                f"    ALTER SERVER AUDIT SPECIFICATION {_bracket(spec.server_audit_spec_name)} WITH (STATE = OFF);",
                "END",
                "GO",
                f"IF EXISTS (SELECT 1 FROM sys.server_audits WHERE name = {_sql_string(spec.audit_name)})",
                "BEGIN",
                f"    ALTER SERVER AUDIT {_bracket(spec.audit_name)} WITH (STATE = OFF);",
                "END",
                "GO",
            ]
        )
        return "\n".join(lines).rstrip() + "\n"

    def render_snapshot(self, spec: SqlServerAuditScriptSpecDto, label: str) -> str:
        lines = [
            f"-- {label.title()} audit snapshot for Benchpress.",
            "SET NOCOUNT ON;",
            "USE [master];",
            "SELECT name, is_state_enabled, type_desc, create_date, modify_date FROM sys.server_audits;",
            "SELECT name, is_state_enabled, create_date, modify_date FROM sys.server_audit_specifications;",
        ]
        for database in spec.selected_databases:
            lines.extend(
                [
                    f"USE {_bracket(database)};",
                    f"SELECT DB_NAME() AS database_name, name, is_state_enabled, create_date, modify_date FROM sys.database_audit_specifications WHERE name = {_sql_string(self._database_spec_name(spec, database))};",
                ]
            )
        return "\n".join(lines).rstrip() + "\n"

    def render_metadata(self, spec: SqlServerAuditScriptSpecDto) -> str:
        return "\n".join(
            [
                "SET NOCOUNT ON;",
                "SELECT SERVERPROPERTY('ProductVersion') AS product_version, SERVERPROPERTY('Edition') AS edition;",
                f"SELECT name, is_state_enabled FROM sys.server_audits WHERE name = {_sql_string(spec.audit_name)};",
                f"SELECT name, is_state_enabled FROM sys.server_audit_specifications WHERE name = {_sql_string(spec.server_audit_spec_name)};",
            ]
        ) + "\n"

    def _drop_database_spec(self, spec: SqlServerAuditScriptSpecDto, database: str) -> list[str]:
        db_spec_name = self._database_spec_name(spec, database)
        return [
            f"USE {_bracket(database)}",
            "GO",
            f"IF EXISTS (SELECT 1 FROM sys.database_audit_specifications WHERE name = {_sql_string(db_spec_name)})",
            "BEGIN",
            f"    ALTER DATABASE AUDIT SPECIFICATION {_bracket(db_spec_name)} WITH (STATE = OFF);",
            f"    DROP DATABASE AUDIT SPECIFICATION {_bracket(db_spec_name)};",
            "END",
            "GO",
            "",
        ]

    def _create_database_spec(self, spec: SqlServerAuditScriptSpecDto, database: str) -> list[str]:
        db_spec_name = self._database_spec_name(spec, database)
        audit_items = list(spec.database_audit_groups)
        audit_items.extend(
            f"{action} ON DATABASE::{_bracket(database)} BY [public]"
            for action in spec.database_statement_actions
        )
        return [
            f"USE {_bracket(database)}",
            "GO",
            f"CREATE DATABASE AUDIT SPECIFICATION {_bracket(db_spec_name)}",
            f"FOR SERVER AUDIT {_bracket(spec.audit_name)}",
            self._audit_items(audit_items),
            "WITH (STATE = ON);",
            "GO",
            "",
        ]

    def _filter_clause(self, spec: SqlServerAuditScriptSpecDto) -> str:
        conditions: list[str] = []
        conditions.extend(
            f"    server_principal_name <> {_sql_string(principal)}"
            for principal in spec.excluded_principals
        )
        conditions.extend(
            f"    client_ip <> {_sql_string(client_ip)}"
            for client_ip in spec.excluded_client_ips
        )
        conditions.extend(
            f"    database_name <> {_sql_string(database)}"
            for database in spec.excluded_database_names
        )
        conditions.extend(
            f"    schema_name <> {_sql_string(schema)}"
            for schema in spec.excluded_schema_names
        )
        return "\n    AND ".join(conditions) + ";" if conditions else ""

    def _audit_items(self, items: list[str]) -> str:
        return ",\n".join(f"ADD ({item})" for item in items)

    def _database_spec_name(self, spec: SqlServerAuditScriptSpecDto, database: str) -> str:
        return spec.database_audit_spec_name_template.format(database=database)


class HammerDbTprocCScriptGenerator:
    def render(self, spec: HammerDbTprocCSpecDto, runtime_spec: BenchmarkRuntimeSpecDto) -> str:
        timing = runtime_spec.workload.timings
        return f"""# Generated by Benchpress. HammerDB SQL Server TPROC-C workload.
proc benchpress_arg {{flag default}} {{
    set idx [lsearch $::argv $flag]
    if {{$idx >= 0 && [expr {{$idx + 1}}] < [llength $::argv]}} {{
        return [lindex $::argv [expr {{$idx + 1}}]]
    }}
    return $default
}}

set virtual_users [benchpress_arg "--vu" "{runtime_spec.workload.virtual_user_ladder[0]}"]
set warmup_minutes {timing.warmup_minutes}
set measured_minutes {timing.measured_minutes}
set cooldown_minutes {timing.cooldown_minutes}
set warehouses {spec.warehouses}

puts "virtual_users=$virtual_users"
puts "warehouses=$warehouses"
puts "warmup_minutes=$warmup_minutes"
puts "measured_minutes=$measured_minutes"
puts "cooldown_minutes=$cooldown_minutes"

dbset db mssqls
dbset bm TPC-C
diset connection mssqls_server {_tcl_quote(spec.sql_server)}
diset connection mssqls_port {spec.sql_port}
diset connection mssqls_uid $::env({spec.username_env})
diset connection mssqls_pass $::env({spec.password_env})
diset connection mssqls_database {_tcl_quote(spec.database_name)}
diset tpcc mssqls_count_ware $warehouses
diset tpcc mssqls_num_vu $virtual_users
diset tpcc mssqls_driver {_tcl_quote(spec.driver_mode)}

if {{{str(spec.build_schema).lower()}}} {{
    buildschema
    waittocomplete
}}

loadscript
vuset vu $virtual_users
vuset logtotemp 1
vucreate
vurun
runtimer $measured_minutes
vudestroy

puts "duration_seconds=[expr {{$measured_minutes * 60}}]"
puts "benchmark_status=completed"
exit
"""


class WindowsLogmanMetricsScriptGenerator:
    def render_start(self, spec: WindowsMetricsScriptSpecDto) -> str:
        counters = "\n".join(f'    "{counter}"' for counter in spec.counters)
        return f"""$ErrorActionPreference = "Stop"
$CollectorName = "{_powershell_escape(spec.collector_name)}"
$OutputRoot = "{_powershell_escape(str(spec.output_root))}"
$CountersPath = Join-Path $OutputRoot "benchpress_counters.txt"

New-Item -ItemType Directory -Force -Path $OutputRoot | Out-Null
@(
{counters}
) | Set-Content -Encoding ASCII -Path $CountersPath

$existing = logman query $CollectorName 2>$null
if ($LASTEXITCODE -eq 0) {{
    logman stop $CollectorName 2>$null | Out-Null
    logman delete $CollectorName | Out-Null
}}

logman create counter $CollectorName -cf $CountersPath -si {spec.sample_interval_seconds} -f bincirc -max {spec.max_size_mb} -o (Join-Path $OutputRoot "benchpress_metrics") | Out-Null
logman start $CollectorName | Out-Null
Write-Output "metrics_started=$CollectorName"
"""

    def render_stop(self, spec: WindowsMetricsScriptSpecDto) -> str:
        return f"""$ErrorActionPreference = "Stop"
$CollectorName = "{_powershell_escape(spec.collector_name)}"
$OutputRoot = "{_powershell_escape(str(spec.output_root))}"

$existing = logman query $CollectorName 2>$null
if ($LASTEXITCODE -eq 0) {{
    logman stop $CollectorName | Out-Null
}}

$LatestBlg = Get-ChildItem -Path $OutputRoot -Filter "*.blg" -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending | Select-Object -First 1
if ($LatestBlg -ne $null) {{
    Write-Output "BENCHPRESS_ARTIFACT=$($LatestBlg.FullName)|host_metrics_blg|Windows PerfMon BLG metrics"
    $CsvPath = [System.IO.Path]::ChangeExtension($LatestBlg.FullName, ".csv")
    relog $LatestBlg.FullName -f CSV -o $CsvPath 2>$null | Out-Null
    if (Test-Path $CsvPath) {{
        Write-Output "BENCHPRESS_ARTIFACT=$CsvPath|host_metrics_csv|Windows PerfMon CSV metrics"
    }}
}}
Write-Output "metrics_stopped=$CollectorName"
"""


def _agent_config(spec: AssetGenerationSpecDto, assets: GeneratedBenchmarkAssets) -> dict[str, object]:
    return {
        "bearer_token_env": "BENCHPRESS_AGENT_TOKEN",
        "sql_connection_name": spec.sql_connection_name,
        "sqlcmd_path": spec.sqlcmd_path,
        "staging_root": str(spec.staging_root),
        "command_timeout_seconds": spec.command_timeout_seconds,
        "enable_audit_sql_file": str(assets.audit_enable_sql),
        "disable_audit_sql_file": str(assets.audit_disable_sql),
        "pre_snapshot_sql_file": str(assets.audit_pre_snapshot_sql),
        "post_snapshot_sql_file": str(assets.audit_post_snapshot_sql),
        "sanity_check_sql": "SELECT @@VERSION AS sqlserver_version",
        "database_metadata_sql_file": str(assets.audit_metadata_sql),
        "metrics_start_command": [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(assets.metrics_start_ps1),
        ],
        "metrics_stop_command": [
            "powershell",
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-File",
            str(assets.metrics_stop_ps1),
        ],
        "filesystem_stats_command": [
            "powershell",
            "-NoProfile",
            "-Command",
            "Get-PSDrive -PSProvider FileSystem | ConvertTo-Json",
        ],
        "host_metadata_command": [
            "powershell",
            "-NoProfile",
            "-Command",
            "Get-ComputerInfo | ConvertTo-Json",
        ],
    }


def _bracket(identifier: str) -> str:
    return "[" + identifier.replace("]", "]]") + "]"


def _sql_string(value: str) -> str:
    return "N'" + value.replace("'", "''") + "'"


def _tcl_quote(value: str) -> str:
    return "{" + value.replace("}", "\\}") + "}"


def _powershell_escape(value: str) -> str:
    return value.replace("`", "``").replace('"', '`"')

