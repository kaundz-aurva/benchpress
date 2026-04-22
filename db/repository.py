from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

from db.migrations import initialize_schema
from orchestration.dto import RunUpdateRequest
from orchestration.models import (
    AuditProfile,
    BenchmarkProfile,
    ErrorRecord,
    HostDefinition,
    RunArtifact,
    RunRecord,
    RunSummary,
    RunPhase,
    RunStatus,
    WorkloadProfile,
    utc_now_iso,
)


class BenchmarkRepository:
    def __init__(self, db_path: Path | str) -> None:
        self.db_path = Path(db_path)
        if self.db_path.parent and not self.db_path.parent.exists():
            raise ValueError(f"database parent directory does not exist: {self.db_path.parent}")
        self.connection = sqlite3.connect(self.db_path)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")

    def close(self) -> None:
        self.connection.close()

    def create_schema(self) -> None:
        initialize_schema(self.connection)

    def create_benchmark_profile(self, profile: BenchmarkProfile) -> BenchmarkProfile:
        cursor = self.connection.execute(
            """
            INSERT INTO benchmark_profiles
                (name, database_engine, database_version, cloud_provider, description)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                profile.name,
                profile.database_engine,
                profile.database_version,
                profile.cloud_provider,
                profile.description,
            ),
        )
        self.connection.commit()
        return BenchmarkProfile(
            name=profile.name,
            database_engine=profile.database_engine,
            database_version=profile.database_version,
            cloud_provider=profile.cloud_provider,
            description=profile.description,
            profile_id=int(cursor.lastrowid),
        )

    def get_benchmark_profile(self, profile_id: int) -> BenchmarkProfile | None:
        row = self.connection.execute(
            "SELECT * FROM benchmark_profiles WHERE id = ?",
            (profile_id,),
        ).fetchone()
        if row is None:
            return None
        return BenchmarkProfile(
            name=row["name"],
            database_engine=row["database_engine"],
            database_version=row["database_version"],
            cloud_provider=row["cloud_provider"],
            description=row["description"],
            profile_id=row["id"],
        )

    def create_host(self, host: HostDefinition) -> HostDefinition:
        cursor = self.connection.execute(
            """
            INSERT INTO hosts
                (name, role, os_type, hostname, vcpus, memory_gb, cloud_instance_id, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                host.name,
                host.role.value,
                host.os_type,
                host.hostname,
                host.vcpus,
                host.memory_gb,
                host.cloud_instance_id,
                _json_dumps(host.metadata),
            ),
        )
        self.connection.commit()
        return HostDefinition(
            name=host.name,
            role=host.role,
            os_type=host.os_type,
            hostname=host.hostname,
            vcpus=host.vcpus,
            memory_gb=host.memory_gb,
            cloud_instance_id=host.cloud_instance_id,
            metadata=host.metadata,
            host_id=int(cursor.lastrowid),
        )

    def get_host(self, host_id: int) -> HostDefinition | None:
        row = self.connection.execute("SELECT * FROM hosts WHERE id = ?", (host_id,)).fetchone()
        if row is None:
            return None
        return HostDefinition(
            name=row["name"],
            role=row["role"],
            os_type=row["os_type"],
            hostname=row["hostname"],
            vcpus=row["vcpus"],
            memory_gb=row["memory_gb"],
            cloud_instance_id=row["cloud_instance_id"],
            metadata=_json_loads(row["metadata_json"]),
            host_id=row["id"],
        )

    def create_workload_profile(self, profile: WorkloadProfile) -> WorkloadProfile:
        cursor = self.connection.execute(
            """
            INSERT INTO workload_profiles
                (name, tool, virtual_users, warmup_minutes, measured_minutes, cooldown_minutes, metadata_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                profile.name,
                profile.tool,
                profile.virtual_users,
                profile.warmup_minutes,
                profile.measured_minutes,
                profile.cooldown_minutes,
                _json_dumps(profile.metadata),
            ),
        )
        self.connection.commit()
        return WorkloadProfile(
            name=profile.name,
            tool=profile.tool,
            virtual_users=profile.virtual_users,
            warmup_minutes=profile.warmup_minutes,
            measured_minutes=profile.measured_minutes,
            cooldown_minutes=profile.cooldown_minutes,
            metadata=profile.metadata,
            workload_profile_id=int(cursor.lastrowid),
        )

    def get_workload_profile(self, profile_id: int) -> WorkloadProfile | None:
        row = self.connection.execute(
            "SELECT * FROM workload_profiles WHERE id = ?",
            (profile_id,),
        ).fetchone()
        if row is None:
            return None
        return WorkloadProfile(
            name=row["name"],
            tool=row["tool"],
            virtual_users=row["virtual_users"],
            warmup_minutes=row["warmup_minutes"],
            measured_minutes=row["measured_minutes"],
            cooldown_minutes=row["cooldown_minutes"],
            metadata=_json_loads(row["metadata_json"]),
            workload_profile_id=row["id"],
        )

    def create_audit_profile(self, profile: AuditProfile) -> AuditProfile:
        cursor = self.connection.execute(
            """
            INSERT INTO audit_profiles (name, mode, config_json)
            VALUES (?, ?, ?)
            """,
            (profile.name, profile.mode.value, _json_dumps(profile.config)),
        )
        self.connection.commit()
        return AuditProfile(
            name=profile.name,
            mode=profile.mode,
            config=profile.config,
            audit_profile_id=int(cursor.lastrowid),
        )

    def get_audit_profile(self, profile_id: int) -> AuditProfile | None:
        row = self.connection.execute(
            "SELECT * FROM audit_profiles WHERE id = ?",
            (profile_id,),
        ).fetchone()
        if row is None:
            return None
        return AuditProfile(
            name=row["name"],
            mode=row["mode"],
            config=_json_loads(row["config_json"]),
            audit_profile_id=row["id"],
        )

    def create_run(self, run: RunRecord) -> RunRecord:
        cursor = self.connection.execute(
            """
            INSERT INTO runs
                (
                    benchmark_profile_id, target_host_id, client_host_id,
                    workload_profile_id, audit_profile_id, repetition,
                    status, phase, output_dir, created_at, updated_at
                )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run.benchmark_profile_id,
                run.target_host_id,
                run.client_host_id,
                run.workload_profile_id,
                run.audit_profile_id,
                run.repetition,
                run.status.value,
                run.phase.value,
                str(run.output_dir),
                run.created_at,
                run.updated_at,
            ),
        )
        self.connection.commit()
        return RunRecord(
            benchmark_profile_id=run.benchmark_profile_id,
            target_host_id=run.target_host_id,
            client_host_id=run.client_host_id,
            workload_profile_id=run.workload_profile_id,
            audit_profile_id=run.audit_profile_id,
            repetition=run.repetition,
            output_dir=run.output_dir,
            status=run.status,
            phase=run.phase,
            created_at=run.created_at,
            updated_at=run.updated_at,
            run_id=int(cursor.lastrowid),
        )

    def get_run(self, run_id: int) -> RunRecord | None:
        row = self.connection.execute("SELECT * FROM runs WHERE id = ?", (run_id,)).fetchone()
        if row is None:
            return None
        return _run_record_from_row(row)

    def update_run(self, request: RunUpdateRequest) -> RunRecord:
        current = self.get_run(request.run_id)
        if current is None:
            raise KeyError(f"run not found: {request.run_id}")
        fields: list[str] = []
        values: list[Any] = []
        if request.status is not None:
            fields.append("status = ?")
            values.append(request.status.value)
        if request.phase is not None:
            fields.append("phase = ?")
            values.append(request.phase.value)
        fields.append("updated_at = ?")
        values.append(utc_now_iso())
        values.append(request.run_id)
        self.connection.execute(
            f"UPDATE runs SET {', '.join(fields)} WHERE id = ?",
            values,
        )
        self.connection.commit()
        updated = self.get_run(request.run_id)
        if updated is None:
            raise KeyError(f"run not found after update: {request.run_id}")
        return updated

    def update_run_status_phase(
        self,
        run_id: int,
        status: RunStatus | str | None = None,
        phase: RunPhase | str | None = None,
    ) -> RunRecord:
        return self.update_run(RunUpdateRequest(run_id=run_id, status=status, phase=phase))

    def register_artifact(self, artifact: RunArtifact) -> RunArtifact:
        cursor = self.connection.execute(
            """
            INSERT INTO run_artifacts (run_id, artifact_type, path, description, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                artifact.run_id,
                artifact.artifact_type,
                str(artifact.path),
                artifact.description,
                artifact.created_at,
            ),
        )
        self.connection.commit()
        return RunArtifact(
            run_id=artifact.run_id,
            artifact_type=artifact.artifact_type,
            path=artifact.path,
            description=artifact.description,
            created_at=artifact.created_at,
            artifact_id=int(cursor.lastrowid),
        )

    def list_artifacts(self, run_id: int) -> list[RunArtifact]:
        rows = self.connection.execute(
            "SELECT * FROM run_artifacts WHERE run_id = ? ORDER BY id",
            (run_id,),
        ).fetchall()
        return [
            RunArtifact(
                run_id=row["run_id"],
                artifact_type=row["artifact_type"],
                path=Path(row["path"]),
                description=row["description"],
                created_at=row["created_at"],
                artifact_id=row["id"],
            )
            for row in rows
        ]

    def save_summary(self, summary: RunSummary) -> RunSummary:
        cursor = self.connection.execute(
            """
            INSERT INTO run_summaries (run_id, metrics_json, notes, created_at)
            VALUES (?, ?, ?, ?)
            """,
            (
                summary.run_id,
                _json_dumps(summary.metrics),
                summary.notes,
                summary.created_at,
            ),
        )
        self.connection.commit()
        return RunSummary(
            run_id=summary.run_id,
            metrics=summary.metrics,
            notes=summary.notes,
            created_at=summary.created_at,
            summary_id=int(cursor.lastrowid),
        )

    def get_summary(self, run_id: int) -> RunSummary | None:
        row = self.connection.execute(
            "SELECT * FROM run_summaries WHERE run_id = ?",
            (run_id,),
        ).fetchone()
        if row is None:
            return None
        return RunSummary(
            run_id=row["run_id"],
            metrics=_json_loads(row["metrics_json"]),
            notes=row["notes"],
            created_at=row["created_at"],
            summary_id=row["id"],
        )

    def save_error(self, error: ErrorRecord) -> ErrorRecord:
        cursor = self.connection.execute(
            """
            INSERT INTO run_errors (run_id, phase, message, exception_type, created_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (
                error.run_id,
                error.phase.value,
                error.message,
                error.exception_type,
                error.created_at,
            ),
        )
        self.connection.commit()
        return ErrorRecord(
            run_id=error.run_id,
            phase=error.phase,
            message=error.message,
            exception_type=error.exception_type,
            created_at=error.created_at,
            error_id=int(cursor.lastrowid),
        )

    def list_errors(self, run_id: int) -> list[ErrorRecord]:
        rows = self.connection.execute(
            "SELECT * FROM run_errors WHERE run_id = ? ORDER BY id",
            (run_id,),
        ).fetchall()
        return [
            ErrorRecord(
                run_id=row["run_id"],
                phase=row["phase"],
                message=row["message"],
                exception_type=row["exception_type"],
                created_at=row["created_at"],
                error_id=row["id"],
            )
            for row in rows
        ]


def _json_dumps(value: dict[str, Any]) -> str:
    return json.dumps(value, sort_keys=True)


def _json_loads(value: str) -> dict[str, Any]:
    loaded = json.loads(value)
    if not isinstance(loaded, dict):
        raise ValueError("stored JSON value is not an object")
    return loaded


def _run_record_from_row(row: sqlite3.Row) -> RunRecord:
    return RunRecord(
        benchmark_profile_id=row["benchmark_profile_id"],
        target_host_id=row["target_host_id"],
        client_host_id=row["client_host_id"],
        workload_profile_id=row["workload_profile_id"],
        audit_profile_id=row["audit_profile_id"],
        repetition=row["repetition"],
        output_dir=Path(row["output_dir"]),
        status=row["status"],
        phase=row["phase"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        run_id=row["id"],
    )

