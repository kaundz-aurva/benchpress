from __future__ import annotations

import sqlite3
import tempfile
import unittest
from pathlib import Path

from db.constants import TABLE_RUNS
from db.repository import BenchmarkRepository
from orchestration.models import (
    AuditProfile,
    BenchmarkProfile,
    ErrorRecord,
    HostDefinition,
    HostRole,
    RunArtifact,
    RunPhase,
    RunRecord,
    RunStatus,
    RunSummary,
    WorkloadProfile,
)


class RepositoryTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.repo = BenchmarkRepository(Path(self.temp_dir.name) / "benchpress.sqlite3")
        self.repo.create_schema()

    def tearDown(self) -> None:
        self.repo.close()
        self.temp_dir.cleanup()

    def _seed_run(self) -> RunRecord:
        benchmark = self.repo.create_benchmark_profile(BenchmarkProfile("bench"))
        target = self.repo.create_host(
            HostDefinition("sql", HostRole.TARGET, "windows", "sql", 4, 16)
        )
        client = self.repo.create_host(
            HostDefinition("client", HostRole.CLIENT, "windows", "client", 2, 4)
        )
        workload = self.repo.create_workload_profile(WorkloadProfile("hammerdb_10vu"))
        audit = self.repo.create_audit_profile(AuditProfile("audit_off", "audit_off"))
        return self.repo.create_run(
            RunRecord(
                benchmark_profile_id=benchmark.profile_id or 0,
                target_host_id=target.host_id or 0,
                client_host_id=client.host_id or 0,
                workload_profile_id=workload.workload_profile_id or 0,
                audit_profile_id=audit.audit_profile_id or 0,
                repetition=1,
                output_dir=Path(self.temp_dir.name) / "out",
            )
        )

    def test_schema_creation_is_deterministic(self) -> None:
        self.repo.create_schema()
        row = self.repo.connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = ?",
            (TABLE_RUNS,),
        ).fetchone()
        self.assertIsNotNone(row)

    def test_create_get_update_run_flow(self) -> None:
        run = self._seed_run()
        stored = self.repo.get_run(run.run_id or 0)
        updated = self.repo.update_run_status_phase(
            run.run_id or 0,
            status=RunStatus.RUNNING,
            phase=RunPhase.WORKLOAD_RUN,
        )

        self.assertIsNotNone(stored)
        self.assertEqual(stored.status, RunStatus.PENDING)
        self.assertEqual(updated.status, RunStatus.RUNNING)
        self.assertEqual(updated.phase, RunPhase.WORKLOAD_RUN)

    def test_update_missing_run_raises(self) -> None:
        with self.assertRaises(KeyError):
            self.repo.update_run_status_phase(999, status=RunStatus.RUNNING)

    def test_artifact_error_and_summary_persistence(self) -> None:
        run = self._seed_run()
        artifact = self.repo.register_artifact(
            RunArtifact(run_id=run.run_id or 0, artifact_type="raw", path="raw.txt")
        )
        summary = self.repo.save_summary(
            RunSummary(run_id=run.run_id or 0, metrics={"tpm": 123}, notes="ok")
        )
        error = self.repo.save_error(
            ErrorRecord(run_id=run.run_id or 0, phase=RunPhase.PRECHECK, message="boom")
        )

        self.assertGreater(artifact.artifact_id or 0, 0)
        self.assertEqual(self.repo.get_summary(run.run_id or 0), summary)
        self.assertEqual(self.repo.list_errors(run.run_id or 0), [error])
        self.assertEqual(len(self.repo.list_artifacts(run.run_id or 0)), 1)

    def test_foreign_key_errors_surface(self) -> None:
        with self.assertRaises(sqlite3.IntegrityError):
            self.repo.create_run(
                RunRecord(
                    benchmark_profile_id=1,
                    target_host_id=1,
                    client_host_id=1,
                    workload_profile_id=1,
                    audit_profile_id=1,
                    repetition=1,
                    output_dir="out",
                )
            )


if __name__ == "__main__":
    unittest.main()

