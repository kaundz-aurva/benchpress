from __future__ import annotations

import unittest
from pathlib import Path

from adapters.database.dto import SnapshotRequest
from adapters.transport.dto import RemoteCommandRequest, RemoteCommandResult
from adapters.workload.dto import WorkloadExecutionRequest, WorkloadExecutionResult
from orchestration.dto import ArtifactRegistrationRequest, RunUpdateRequest
from orchestration.models import AuditProfile, HostDefinition, HostRole, WorkloadProfile


class DtoTests(unittest.TestCase):
    def setUp(self) -> None:
        self.target = HostDefinition("sql", HostRole.TARGET, "windows", "sql", 4, 16)
        self.client = HostDefinition("client", HostRole.CLIENT, "windows", "client", 2, 4)
        self.workload = WorkloadProfile("hammerdb_10vu")
        self.audit = AuditProfile("audit_off", "audit_off")

    def test_remote_command_defaults_and_success_property(self) -> None:
        request = RemoteCommandRequest(host=self.target, command="hostname")
        result = RemoteCommandResult(command=request.command, exit_code=0)

        self.assertEqual(request.timeout_seconds, 60)
        self.assertTrue(result.succeeded)

    def test_remote_command_rejects_missing_command(self) -> None:
        with self.assertRaises(ValueError):
            RemoteCommandRequest(host=self.target, command=" ")

    def test_workload_result_requires_error_for_failure(self) -> None:
        with self.assertRaises(ValueError):
            WorkloadExecutionResult(success=False)
        result = WorkloadExecutionResult(success=True, artifacts=("out.txt",), metrics={"tpm": 1})
        self.assertEqual(result.artifacts, (Path("out.txt"),))

    def test_request_dtos_validate_required_fields(self) -> None:
        workload_request = WorkloadExecutionRequest(
            run_id=1,
            workload_profile=self.workload,
            target_host=self.target,
            client_host=self.client,
            audit_profile=self.audit,
            output_dir="out",
        )
        snapshot_request = SnapshotRequest(
            run_id=1,
            host=self.target,
            output_dir="out",
            label="pre",
        )
        artifact_request = ArtifactRegistrationRequest(
            run_id=1,
            artifact_type="raw",
            path="out/raw.txt",
        )
        update = RunUpdateRequest(run_id=1, status="running")

        self.assertEqual(workload_request.output_dir, Path("out"))
        self.assertEqual(snapshot_request.label, "pre")
        self.assertEqual(artifact_request.path, Path("out/raw.txt"))
        self.assertEqual(update.status.value, "running")

    def test_run_update_requires_status_or_phase(self) -> None:
        with self.assertRaises(ValueError):
            RunUpdateRequest(run_id=1)


if __name__ == "__main__":
    unittest.main()

