from __future__ import annotations

import unittest
from pathlib import Path

from orchestration.models import (
    AuditMode,
    AuditProfile,
    BenchmarkProfile,
    ErrorRecord,
    HostDefinition,
    HostRole,
    RunArtifact,
    RunPhase,
    RunRecord,
    RunSpec,
    RunStatus,
    RunSummary,
    WorkloadProfile,
)


class DomainModelTests(unittest.TestCase):
    def test_valid_domain_model_construction(self) -> None:
        benchmark = BenchmarkProfile(name="sqlserver-audit")
        target = HostDefinition(
            name="sql",
            role=HostRole.TARGET,
            os_type="windows",
            hostname="sql.example",
            vcpus=4,
            memory_gb=16,
        )
        client = HostDefinition(
            name="client",
            role="client",
            os_type="windows",
            hostname="client.example",
            vcpus=2,
            memory_gb=4,
        )
        workload = WorkloadProfile(name="hammerdb_10vu", virtual_users=10)
        audit = AuditProfile(name="audit_on", mode="audit_on")
        spec = RunSpec(
            benchmark_profile=benchmark,
            target_host=target,
            client_host=client,
            workload_profile=workload,
            audit_profile=audit,
            repetition=1,
            output_root=Path("out"),
        )

        self.assertEqual(spec.audit_profile.mode, AuditMode.AUDIT_ON)
        self.assertEqual(spec.target_host.role, HostRole.TARGET)

    def test_invalid_required_fields_fail_early(self) -> None:
        with self.assertRaises(ValueError):
            BenchmarkProfile(name="")
        with self.assertRaises(ValueError):
            WorkloadProfile(name="bad", virtual_users=0)
        with self.assertRaises(ValueError):
            HostDefinition(
                name="host",
                role="target",
                os_type="windows",
                hostname="h",
                vcpus=0,
                memory_gb=16,
            )

    def test_run_spec_rejects_swapped_hosts(self) -> None:
        host = HostDefinition(
            name="client",
            role=HostRole.CLIENT,
            os_type="windows",
            hostname="client",
            vcpus=2,
            memory_gb=4,
        )
        with self.assertRaises(ValueError):
            RunSpec(
                benchmark_profile=BenchmarkProfile(name="bench"),
                target_host=host,
                client_host=host,
                workload_profile=WorkloadProfile(name="workload"),
                audit_profile=AuditProfile(name="audit", mode=AuditMode.AUDIT_OFF),
                repetition=1,
                output_root=Path("out"),
            )

    def test_run_models_convert_enums_and_paths(self) -> None:
        run = RunRecord(
            benchmark_profile_id=1,
            target_host_id=2,
            client_host_id=3,
            workload_profile_id=4,
            audit_profile_id=5,
            repetition=1,
            output_dir="out",
            status="running",
            phase="precheck",
        )
        artifact = RunArtifact(run_id=1, artifact_type="raw", path="raw.txt")
        summary = RunSummary(run_id=1, metrics={"tpm": 100})
        error = ErrorRecord(run_id=1, phase="precheck", message="failed")

        self.assertEqual(run.status, RunStatus.RUNNING)
        self.assertEqual(run.phase, RunPhase.PRECHECK)
        self.assertEqual(artifact.path, Path("raw.txt"))
        self.assertEqual(summary.metrics["tpm"], 100)
        self.assertEqual(error.phase, RunPhase.PRECHECK)


if __name__ == "__main__":
    unittest.main()

