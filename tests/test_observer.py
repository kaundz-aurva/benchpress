from __future__ import annotations

import json
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from typing import Any

from benchpress_observer import _session_config_from_args, main as observer_main
from db.repository import BenchmarkRepository
from observer.commands import CommandParseError, parse_command
from observer.service import ObserverService
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


class ObserverTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.db_path = self.root / "benchpress.sqlite3"
        self.repo = BenchmarkRepository(self.db_path)
        self.repo.create_schema()

    def tearDown(self) -> None:
        self.repo.close()
        self.temp_dir.cleanup()

    def test_parse_command_supports_navigation_and_refresh(self) -> None:
        self.assertEqual(parse_command(":q").name, "quit")
        self.assertEqual(parse_command(":runs").name, "runs")
        self.assertEqual(parse_command(":dashboard").name, "dashboard")
        self.assertEqual(parse_command(":reload").name, "reload")
        self.assertEqual(parse_command(":open 12").value, 12)
        self.assertEqual(parse_command(":refresh 5").value, 5.0)

    def test_parse_command_rejects_invalid_values(self) -> None:
        with self.assertRaises(CommandParseError):
            parse_command(":open nope")
        with self.assertRaises(CommandParseError):
            parse_command(":refresh 0")
        with self.assertRaises(CommandParseError):
            parse_command(":unknown")

    def test_service_loads_snapshot_counts_and_artifact_fallback(self) -> None:
        self._seed_run(
            audit_mode="audit_off",
            virtual_users=10,
            metrics=None,
            artifact_text="tpm=120\nnopm=95\n",
            status=RunStatus.RUNNING,
            phase=RunPhase.WORKLOAD_RUN,
        )
        failed_run = self._seed_run(
            audit_mode="audit_on",
            virtual_users=10,
            metrics={"tpm": 90},
            status=RunStatus.FAILED,
            phase=RunPhase.METRICS_STOP,
            error=("RuntimeError", "metrics stop failed"),
        )

        snapshot = ObserverService().load_snapshot(
            _session_config_from_args(
                Namespace(
                    db=str(self.db_path),
                    spec=None,
                    artifact_root=str(self.root),
                    refresh_seconds=2.0,
                )
            )
        )

        self.assertEqual(snapshot.status_counts["running"], 1)
        self.assertEqual(snapshot.status_counts["failed"], 1)
        self.assertEqual(snapshot.phase_counts["workload_run"], 1)
        self.assertEqual(snapshot.runs[0].workload_metrics["tpm"], 120)
        self.assertEqual(snapshot.runs[0].workload_metrics["nopm"], 95)
        self.assertEqual(snapshot.recent_failures[0].run_id, failed_run.run_id)

    def test_service_previews_small_text_artifacts_only(self) -> None:
        run = self._seed_run(
            audit_mode="audit_off",
            virtual_users=20,
            metrics={"tpm": 200},
            artifact_text='{"hello":"world"}\n',
            artifact_name="summary.json",
        )
        snapshot = ObserverService().load_snapshot(
            _session_config_from_args(
                Namespace(
                    db=str(self.db_path),
                    spec=None,
                    artifact_root=str(self.root),
                    refresh_seconds=2.0,
                )
            )
        )
        run_state = snapshot.find_run(run.run_id)
        self.assertIsNotNone(run_state)
        preview = ObserverService().preview_artifact(
            run_state,
            0,
            _session_config_from_args(
                Namespace(
                    db=str(self.db_path),
                    spec=None,
                    artifact_root=str(self.root),
                    refresh_seconds=2.0,
                )
            ),
        )

        self.assertTrue(preview.previewable)
        self.assertIn('"hello": "world"', preview.text)

    def test_service_cache_invalidates_when_artifact_content_changes(self) -> None:
        run = self._seed_run(
            audit_mode="audit_off",
            virtual_users=25,
            metrics=None,
            artifact_text="tpm=111\n",
            status=RunStatus.RUNNING,
            phase=RunPhase.WORKLOAD_RUN,
        )
        service = ObserverService()
        session_config = _session_config_from_args(
            Namespace(
                db=str(self.db_path),
                spec=None,
                artifact_root=str(self.root),
                refresh_seconds=2.0,
            )
        )

        first_snapshot = service.load_snapshot(session_config)
        self.assertEqual(first_snapshot.find_run(run.run_id).workload_metrics["tpm"], 111)

        artifact_path = run.output_dir / "hammerdb_stdout.txt"
        artifact_path.write_text("tpm=222\n", encoding="utf-8")

        second_snapshot = service.load_snapshot(session_config)
        self.assertEqual(second_snapshot.find_run(run.run_id).workload_metrics["tpm"], 222)

    def test_service_rejects_large_artifact_preview(self) -> None:
        run = self._seed_run(
            audit_mode="audit_off",
            virtual_users=30,
            metrics={"tpm": 300},
            artifact_text="x" * 70000,
            artifact_name="large.txt",
        )
        session_config = _session_config_from_args(
            Namespace(
                db=str(self.db_path),
                spec=None,
                artifact_root=str(self.root),
                refresh_seconds=2.0,
            )
        )
        snapshot = ObserverService().load_snapshot(session_config)
        run_state = snapshot.find_run(run.run_id)
        self.assertIsNotNone(run_state)
        preview = ObserverService().preview_artifact(run_state, 0, session_config)

        self.assertFalse(preview.previewable)
        self.assertIn("preview limit", preview.reason)

    def test_cli_uses_db_parent_by_default(self) -> None:
        captured: list[object] = []

        exit_code = observer_main(
            ["--db", str(self.db_path)],
            launch_fn=lambda config: captured.append(config),
        )

        self.assertEqual(exit_code, 0)
        self.assertEqual(captured[0].db_path, self.db_path)
        self.assertEqual(captured[0].artifact_root, self.root)

    def test_cli_uses_spec_storage_paths(self) -> None:
        spec_path = self.root / "benchmark.json"
        spec_path.write_text(json.dumps(self._spec()), encoding="utf-8")
        captured: list[object] = []

        exit_code = observer_main(
            ["--spec", str(spec_path)],
            launch_fn=lambda config: captured.append(config),
        )

        self.assertEqual(exit_code, 0)
        self.assertEqual(captured[0].db_path, self.db_path)
        self.assertEqual(captured[0].artifact_root, self.root / "outputs")

    def _seed_run(
        self,
        audit_mode: str,
        virtual_users: int,
        repetition: int = 1,
        metrics: dict[str, Any] | None = None,
        status: RunStatus = RunStatus.SUCCESS,
        phase: RunPhase = RunPhase.DONE,
        error: tuple[str, str] | None = None,
        artifact_text: str | None = None,
        artifact_name: str = "hammerdb_stdout.txt",
    ) -> RunRecord:
        benchmark = self.repo.create_benchmark_profile(BenchmarkProfile(name="bench"))
        target = self.repo.create_host(
            HostDefinition(
                name="sql",
                role=HostRole.TARGET,
                os_type="windows",
                hostname="sql",
                vcpus=4,
                memory_gb=16,
            )
        )
        client = self.repo.create_host(
            HostDefinition(
                name="client",
                role=HostRole.CLIENT,
                os_type="windows",
                hostname="client",
                vcpus=2,
                memory_gb=4,
            )
        )
        workload = self.repo.create_workload_profile(
            WorkloadProfile(name=f"hammerdb_{virtual_users}", virtual_users=virtual_users)
        )
        audit = self.repo.create_audit_profile(AuditProfile(name=audit_mode, mode=audit_mode))
        output_dir = self.root / audit_mode / f"{virtual_users}vu" / f"rep_{repetition}"
        output_dir.mkdir(parents=True, exist_ok=True)
        run = self.repo.create_run(
            RunRecord(
                benchmark_profile_id=benchmark.profile_id or 0,
                target_host_id=target.host_id or 0,
                client_host_id=client.host_id or 0,
                workload_profile_id=workload.workload_profile_id or 0,
                audit_profile_id=audit.audit_profile_id or 0,
                repetition=repetition,
                output_dir=output_dir,
                status=status,
                phase=phase,
            )
        )
        if metrics is not None:
            self.repo.save_summary(
                RunSummary(
                    run_id=run.run_id or 0,
                    metrics={"workload": metrics, "database": {"engine": "sqlserver"}},
                )
            )
        if artifact_text is not None:
            artifact_path = output_dir / artifact_name
            artifact_path.write_text(artifact_text, encoding="utf-8")
            self.repo.register_artifact(
                RunArtifact(
                    run_id=run.run_id or 0,
                    artifact_type="workload_output",
                    path=artifact_path,
                    description="artifact",
                )
            )
        if error is not None:
            self.repo.save_error(
                ErrorRecord(
                    run_id=run.run_id or 0,
                    phase=phase,
                    exception_type=error[0],
                    message=error[1],
                )
            )
        return run

    def _spec(self) -> dict[str, Any]:
        return {
            "benchmark_profile": {"name": "bench"},
            "target_host": {
                "name": "sql",
                "role": "target",
                "os_type": "windows",
                "hostname": "sql",
                "vcpus": 4,
                "memory_gb": 16,
            },
            "client_host": {
                "name": "client",
                "role": "client",
                "os_type": "windows",
                "hostname": "client",
                "vcpus": 2,
                "memory_gb": 4,
            },
            "agent": {
                "base_url": "http://agent",
                "bearer_token_env": "BENCHPRESS_AGENT_TOKEN",
            },
            "workload": {
                "hammerdb_executable_path": "hammerdb",
                "hammerdb_script_path": "run.tcl",
                "virtual_user_ladder": [1],
                "timings": {
                    "warmup_minutes": 0,
                    "measured_minutes": 1,
                    "cooldown_minutes": 0,
                },
                "repetitions": 1,
            },
            "audit": {"modes": ["audit_off"]},
            "storage": {
                "sqlite_path": str(self.db_path),
                "output_root": str(self.root / "outputs"),
            },
        }


if __name__ == "__main__":
    unittest.main()
