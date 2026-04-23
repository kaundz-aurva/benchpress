from __future__ import annotations

import json
import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any

from reporting.models import ReportArtifact, ReportError, ReportSourceRun


class ReportingRepository:
    def __init__(self, db_path: Path | str) -> None:
        self.db_path = Path(db_path)
        if not self.db_path.exists():
            raise FileNotFoundError(self.db_path)
        self.connection = sqlite3.connect(self.db_path)
        self.connection.row_factory = sqlite3.Row

    def __enter__(self) -> ReportingRepository:
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        self.close()

    def close(self) -> None:
        self.connection.close()

    def list_runs(self) -> list[ReportSourceRun]:
        run_rows = self.connection.execute(
            """
            SELECT
                runs.id AS run_id,
                runs.repetition,
                runs.status,
                runs.phase,
                runs.output_dir,
                runs.created_at,
                runs.updated_at,
                benchmark_profiles.name AS benchmark_name,
                benchmark_profiles.database_engine,
                benchmark_profiles.database_version,
                benchmark_profiles.cloud_provider,
                workload_profiles.name AS workload_name,
                workload_profiles.tool AS workload_tool,
                workload_profiles.virtual_users,
                audit_profiles.name AS audit_name,
                audit_profiles.mode AS audit_mode,
                run_summaries.metrics_json,
                run_summaries.notes AS summary_notes
            FROM runs
            JOIN benchmark_profiles
                ON benchmark_profiles.id = runs.benchmark_profile_id
            JOIN workload_profiles
                ON workload_profiles.id = runs.workload_profile_id
            JOIN audit_profiles
                ON audit_profiles.id = runs.audit_profile_id
            LEFT JOIN run_summaries
                ON run_summaries.run_id = runs.id
            ORDER BY workload_profiles.virtual_users, audit_profiles.mode, runs.repetition, runs.id
            """
        ).fetchall()
        artifacts = self._artifacts_by_run()
        errors = self._errors_by_run()
        return [
            ReportSourceRun(
                run_id=row["run_id"],
                benchmark_name=row["benchmark_name"],
                database_engine=row["database_engine"],
                database_version=row["database_version"],
                cloud_provider=row["cloud_provider"],
                workload_name=row["workload_name"],
                workload_tool=row["workload_tool"],
                virtual_users=row["virtual_users"],
                repetition=row["repetition"],
                audit_name=row["audit_name"],
                audit_mode=row["audit_mode"],
                status=row["status"],
                phase=row["phase"],
                output_dir=Path(row["output_dir"]),
                created_at=row["created_at"],
                updated_at=row["updated_at"],
                summary_metrics=_json_object(row["metrics_json"]),
                summary_notes=row["summary_notes"] or "",
                artifacts=tuple(artifacts.get(row["run_id"], ())),
                errors=tuple(errors.get(row["run_id"], ())),
            )
            for row in run_rows
        ]

    def _artifacts_by_run(self) -> dict[int, list[ReportArtifact]]:
        rows = self.connection.execute(
            """
            SELECT id, run_id, artifact_type, path, description, created_at
            FROM run_artifacts
            ORDER BY run_id, id
            """
        ).fetchall()
        artifacts: dict[int, list[ReportArtifact]] = defaultdict(list)
        for row in rows:
            artifacts[row["run_id"]].append(
                ReportArtifact(
                    artifact_id=row["id"],
                    run_id=row["run_id"],
                    artifact_type=row["artifact_type"],
                    path=Path(row["path"]),
                    description=row["description"],
                    created_at=row["created_at"],
                )
            )
        return artifacts

    def _errors_by_run(self) -> dict[int, list[ReportError]]:
        rows = self.connection.execute(
            """
            SELECT id, run_id, phase, message, exception_type, created_at
            FROM run_errors
            ORDER BY run_id, id
            """
        ).fetchall()
        errors: dict[int, list[ReportError]] = defaultdict(list)
        for row in rows:
            errors[row["run_id"]].append(
                ReportError(
                    error_id=row["id"],
                    run_id=row["run_id"],
                    phase=row["phase"],
                    message=row["message"],
                    exception_type=row["exception_type"],
                    created_at=row["created_at"],
                )
            )
        return errors


def _json_object(value: str | None) -> dict[str, Any]:
    if value is None:
        return {}
    loaded = json.loads(value)
    if not isinstance(loaded, dict):
        raise ValueError("run_summaries.metrics_json must contain a JSON object")
    return loaded
