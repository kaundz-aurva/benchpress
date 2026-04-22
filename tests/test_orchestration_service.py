from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Any

from adapters.database.dto import SnapshotRequest
from adapters.database.service import DatabaseAdapter
from adapters.host.service import HostAdapter
from adapters.workload.dto import WorkloadExecutionRequest, WorkloadExecutionResult
from adapters.workload.service import WorkloadRunner
from config.models import BenchmarkConfig
from db.repository import BenchmarkRepository
from orchestration.models import (
    AuditMode,
    AuditProfile,
    BenchmarkProfile,
    HostDefinition,
    HostRole,
    RunArtifact,
    RunPhase,
    RunStatus,
    WorkloadProfile,
)
from orchestration.service import BenchmarkOrchestrationService


class FakeDatabaseAdapter(DatabaseAdapter):
    def __init__(self, fail_connectivity: bool = False) -> None:
        self.fail_connectivity = fail_connectivity
        self.audit_modes: list[AuditMode] = []

    def validate_connectivity(self) -> bool:
        if self.fail_connectivity:
            raise RuntimeError("database unavailable")
        return True

    def enable_audit(self, audit_profile: AuditProfile) -> None:
        self.audit_modes.append(audit_profile.mode)

    def disable_audit(self, audit_profile: AuditProfile) -> None:
        self.audit_modes.append(audit_profile.mode)

    def run_sanity_checks(self) -> dict[str, Any]:
        return {"ok": True}

    def capture_pre_snapshot(self, request: SnapshotRequest) -> list[RunArtifact]:
        path = request.output_dir / "pre.txt"
        path.write_text("pre", encoding="utf-8")
        return [RunArtifact(request.run_id, "database_pre_snapshot", path)]

    def capture_post_snapshot(self, request: SnapshotRequest) -> list[RunArtifact]:
        path = request.output_dir / "post.txt"
        path.write_text("post", encoding="utf-8")
        return [RunArtifact(request.run_id, "database_post_snapshot", path)]

    def collect_database_metadata(self) -> dict[str, Any]:
        return {"engine": "sqlserver"}


class FakeHostAdapter(HostAdapter):
    def start_metrics_collection(self, run_id: int, output_dir: Path) -> None:
        return None

    def stop_metrics_collection(self, run_id: int, output_dir: Path) -> list[RunArtifact]:
        path = output_dir / "metrics.txt"
        path.write_text("metrics", encoding="utf-8")
        return [RunArtifact(run_id, "host_metrics", path)]

    def collect_filesystem_stats(self) -> dict[str, Any]:
        return {"free_gb": 10}

    def collect_host_metadata(self) -> dict[str, Any]:
        return {"os": "windows"}


class FakeWorkloadRunner(WorkloadRunner):
    def prepare_run(self, request: WorkloadExecutionRequest) -> None:
        return None

    def execute_run(self, request: WorkloadExecutionRequest) -> WorkloadExecutionResult:
        path = request.output_dir / "workload.txt"
        path.write_text("workload", encoding="utf-8")
        return WorkloadExecutionResult(
            success=True,
            artifacts=(path,),
            metrics={"tpm": 100},
            raw_output_path=path,
        )

    def parse_results(self, result_path: Path) -> dict[str, Any]:
        return {"tpm": 100}


class OrchestrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.repo = BenchmarkRepository(Path(self.temp_dir.name) / "benchpress.sqlite3")
        self.repo.create_schema()
        self.benchmark = BenchmarkProfile("sqlserver-audit")
        self.target = HostDefinition("sql", HostRole.TARGET, "windows", "sql", 4, 16)
        self.client = HostDefinition("client", HostRole.CLIENT, "windows", "client", 2, 4)

    def tearDown(self) -> None:
        self.repo.close()
        self.temp_dir.cleanup()

    def _spec(self) -> Any:
        return BenchmarkConfig(
            benchmark_profile=self.benchmark,
            target_host=self.target,
            client_host=self.client,
            audit_profiles=(AuditProfile("off", "audit_off"),),
            virtual_user_ladder=(10,),
            repetitions=1,
            output_root=Path(self.temp_dir.name) / "runs",
        )

    def test_run_matrix_generation_delegates_to_config_service(self) -> None:
        service = BenchmarkOrchestrationService(
            repository=self.repo,
            database_adapter=FakeDatabaseAdapter(),
            target_host_adapter=FakeHostAdapter(),
            workload_runner=FakeWorkloadRunner(),
        )

        specs = service.build_run_matrix(self._spec())

        self.assertEqual(len(specs), 1)
        self.assertEqual(specs[0].workload_profile, WorkloadProfile("hammerdb_10vu"))

    def test_successful_single_run_skeleton(self) -> None:
        database = FakeDatabaseAdapter()
        service = BenchmarkOrchestrationService(
            repository=self.repo,
            database_adapter=database,
            target_host_adapter=FakeHostAdapter(),
            workload_runner=FakeWorkloadRunner(),
        )
        spec = service.build_run_matrix(self._spec())[0]

        run = service.execute_single_run(spec)

        self.assertEqual(run.status, RunStatus.SUCCESS)
        self.assertEqual(run.phase, RunPhase.DONE)
        self.assertEqual(database.audit_modes, [AuditMode.AUDIT_OFF])
        self.assertEqual(len(self.repo.list_artifacts(run.run_id or 0)), 4)
        self.assertIsNotNone(self.repo.get_summary(run.run_id or 0))

    def test_failure_path_persists_error_and_failed_status(self) -> None:
        service = BenchmarkOrchestrationService(
            repository=self.repo,
            database_adapter=FakeDatabaseAdapter(fail_connectivity=True),
            target_host_adapter=FakeHostAdapter(),
            workload_runner=FakeWorkloadRunner(),
        )
        spec = service.build_run_matrix(self._spec())[0]

        run = service.execute_single_run(spec)
        errors = self.repo.list_errors(run.run_id or 0)

        self.assertEqual(run.status, RunStatus.FAILED)
        self.assertEqual(run.phase, RunPhase.PRECHECK)
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0].exception_type, "RuntimeError")


if __name__ == "__main__":
    unittest.main()

