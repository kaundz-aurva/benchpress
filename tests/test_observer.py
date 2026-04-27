from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from typing import Any

from benchpress_observer import _session_config_from_args, main as observer_main
from db.repository import BenchmarkRepository
from observer.commands import CommandParseError, ObserverCommand, parse_command
from observer.service import ObserverService
from observer.ui import BenchpressObserverApp
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
        self.assertEqual(parse_command(":failures").name, "failures")
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

        snapshot = ObserverService().load_snapshot(self._session_config())

        self.assertEqual(snapshot.status_counts["running"], 1)
        self.assertEqual(snapshot.status_counts["failed"], 1)
        self.assertEqual(snapshot.phase_counts["workload_run"], 1)
        self.assertEqual(snapshot.runs[0].workload_metrics["tpm"], 120)
        self.assertEqual(snapshot.runs[0].workload_metrics["nopm"], 95)
        self.assertEqual(snapshot.failure_runs[0].run_id, failed_run.run_id)
        self.assertEqual(snapshot.recent_failures[0].run_id, failed_run.run_id)

    def test_service_projects_failure_triage_and_latest_error(self) -> None:
        failed_run = self._seed_run(
            audit_mode="audit_on",
            virtual_users=16,
            metrics={"tpm": 160},
            status=RunStatus.FAILED,
            phase=RunPhase.METRICS_STOP,
        )
        self._save_error(
            failed_run,
            exception_type="RuntimeError",
            message="first failure",
            phase=RunPhase.METRICS_STOP,
        )
        self._save_error(
            failed_run,
            exception_type="RuntimeError",
            message="second failure",
            phase=RunPhase.ARTIFACT_COLLECTION,
        )
        error_only_run = self._seed_run(
            audit_mode="audit_off",
            virtual_users=24,
            metrics={"tpm": 240},
            status=RunStatus.SUCCESS,
            phase=RunPhase.DONE,
        )
        self._save_error(
            error_only_run,
            exception_type="ValueError",
            message="persisted error after success",
            phase=RunPhase.SUMMARIZE,
        )

        snapshot = ObserverService().load_snapshot(self._session_config())

        self.assertEqual(
            [run.run_id for run in snapshot.failure_runs],
            [error_only_run.run_id, failed_run.run_id],
        )
        failed_state = snapshot.find_run(failed_run.run_id)
        self.assertIsNotNone(failed_state)
        self.assertEqual(failed_state.latest_error_message, "second failure")
        error_only_state = snapshot.find_run(error_only_run.run_id)
        self.assertIsNotNone(error_only_state)
        self.assertTrue(error_only_state.has_failures)

    def test_service_previews_small_text_artifacts_only(self) -> None:
        run = self._seed_run(
            audit_mode="audit_off",
            virtual_users=20,
            metrics={"tpm": 200},
            artifact_text='{"hello":"world"}\n',
            artifact_name="summary.json",
        )
        snapshot = ObserverService().load_snapshot(self._session_config())
        run_state = snapshot.find_run(run.run_id)
        self.assertIsNotNone(run_state)
        preview = ObserverService().preview_artifact(
            run_state,
            0,
            self._session_config(),
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
        session_config = self._session_config()

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
        session_config = self._session_config()
        snapshot = ObserverService().load_snapshot(session_config)
        run_state = snapshot.find_run(run.run_id)
        self.assertIsNotNone(run_state)
        preview = ObserverService().preview_artifact(run_state, 0, session_config)

        self.assertFalse(preview.previewable)
        self.assertIn("preview limit", preview.reason)

    def test_service_orders_key_artifacts_for_triage(self) -> None:
        run = self._seed_run(
            audit_mode="audit_on",
            virtual_users=32,
            status=RunStatus.FAILED,
            phase=RunPhase.POST_SNAPSHOT,
        )
        self._register_artifact(run, artifact_type="database_post_snapshot", artifact_name="post.sql")
        self._register_artifact(run, artifact_type="custom_debug", artifact_name="custom.log")
        self._register_artifact(run, artifact_type="workload_output", artifact_name="workload.log")
        self._register_artifact(run, artifact_type="host_metrics_csv", artifact_name="host.csv")
        self._register_artifact(run, artifact_type="database_pre_snapshot", artifact_name="pre.sql")
        self._register_artifact(run, artifact_type="host_metrics", artifact_name="metrics.json")

        snapshot = ObserverService().load_snapshot(self._session_config())
        run_state = snapshot.find_run(run.run_id)
        self.assertIsNotNone(run_state)
        self.assertEqual(
            [artifact.artifact_type for artifact in run_state.triage_artifacts],
            [
                "workload_output",
                "host_metrics",
                "host_metrics_csv",
                "database_pre_snapshot",
                "database_post_snapshot",
                "custom_debug",
            ],
        )

    def test_service_previews_failure_summary_and_latest_error_text(self) -> None:
        run = self._seed_run(
            audit_mode="audit_on",
            virtual_users=40,
            metrics={"tpm": 400},
            status=RunStatus.FAILED,
            phase=RunPhase.METRICS_STOP,
        )
        self._register_artifact(run, artifact_type="workload_output", artifact_name="workload.log")
        self._save_error(
            run,
            exception_type="RuntimeError",
            message="collector failed",
            phase=RunPhase.METRICS_STOP,
        )
        service = ObserverService()
        snapshot = service.load_snapshot(self._session_config())
        run_state = snapshot.find_run(run.run_id)
        self.assertIsNotNone(run_state)

        error_preview = service.preview_latest_error(run_state)
        summary_preview = service.preview_failure_summary(run_state)

        self.assertTrue(error_preview.previewable)
        self.assertIn("collector failed", error_preview.text)
        self.assertTrue(summary_preview.previewable)
        self.assertIn("Latest Error:", summary_preview.text)
        self.assertIn("workload_output", summary_preview.text)

    def test_service_rejects_missing_and_untrusted_artifact_preview(self) -> None:
        missing_run = self._seed_run(
            audit_mode="audit_off",
            virtual_users=48,
            metrics={"tpm": 480},
            artifact_text="temporary artifact\n",
            artifact_name="missing.txt",
        )
        outside_run = self._seed_run(
            audit_mode="audit_on",
            virtual_users=56,
            metrics={"tpm": 560},
        )
        self._register_artifact(
            outside_run,
            artifact_type="workload_output",
            artifact_name="outside.txt",
            artifact_text=None,
            artifact_path=self.root.parent / "outside.txt",
        )

        session_config = self._session_config()
        service = ObserverService()
        snapshot = service.load_snapshot(session_config)
        missing_path = missing_run.output_dir / "missing.txt"
        missing_path.unlink()

        missing_run_state = snapshot.find_run(missing_run.run_id)
        outside_run_state = snapshot.find_run(outside_run.run_id)
        self.assertIsNotNone(missing_run_state)
        self.assertIsNotNone(outside_run_state)

        missing_preview = service.preview_artifact(missing_run_state, 0, session_config)
        outside_preview = service.preview_artifact(outside_run_state, 0, session_config)

        self.assertFalse(missing_preview.previewable)
        self.assertIn("missing", missing_preview.reason)
        self.assertFalse(outside_preview.previewable)
        self.assertIn("outside the trusted artifact root", outside_preview.reason)

    def test_ui_navigates_failure_triage_and_text_viewer(self) -> None:
        failed_run = self._seed_run(
            audit_mode="audit_on",
            virtual_users=64,
            metrics={"tpm": 640},
            status=RunStatus.FAILED,
            phase=RunPhase.METRICS_STOP,
        )
        self._register_artifact(failed_run, artifact_type="workload_output", artifact_name="failed.log")
        self._save_error(
            failed_run,
            exception_type="RuntimeError",
            message="triage me",
            phase=RunPhase.METRICS_STOP,
        )
        app = BenchpressObserverApp(self._session_config())

        asyncio.run(self._assert_failure_triage_navigation(app, failed_run.run_id))

    def test_ui_reconciles_failure_selection_after_refresh(self) -> None:
        resolved_run = self._seed_run(
            audit_mode="audit_off",
            virtual_users=72,
            status=RunStatus.FAILED,
            phase=RunPhase.METRICS_STOP,
        )
        self._register_artifact(resolved_run, artifact_type="workload_output", artifact_name="resolved.log")
        remaining_run = self._seed_run(
            audit_mode="audit_on",
            virtual_users=80,
            status=RunStatus.FAILED,
            phase=RunPhase.METRICS_STOP,
        )
        self._register_artifact(remaining_run, artifact_type="workload_output", artifact_name="remaining.log")
        app = BenchpressObserverApp(self._session_config())

        asyncio.run(
            self._assert_failure_selection_reconciles(
                app,
                resolved_run_id=resolved_run.run_id,
                remaining_run_id=remaining_run.run_id,
            )
        )

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

    async def _assert_failure_triage_navigation(
        self,
        app: BenchpressObserverApp,
        failed_run_id: int,
    ) -> None:
        async with app.run_test() as pilot:
            await pilot.pause()
            app._execute_command(ObserverCommand("failures"))
            await pilot.pause()
            self.assertEqual(app.view_mode, "failures")
            self.assertEqual(app.selected_run_id, failed_run_id)

            await pilot.press("enter")
            await pilot.pause()
            self.assertEqual(app.view_mode, "detail")
            self.assertEqual(app.detail_mode, "triage")

            await pilot.press("e")
            await pilot.pause()
            self.assertEqual(app.view_mode, "text")
            self.assertIsNotNone(app.text_preview)
            self.assertEqual(app.text_preview.source_kind, "error")

            await pilot.press("escape")
            await pilot.pause()
            self.assertEqual(app.view_mode, "detail")

            await pilot.press("enter")
            await pilot.pause()
            self.assertEqual(app.view_mode, "text")
            self.assertIsNotNone(app.text_preview)
            self.assertEqual(app.text_preview.source_kind, "artifact")

            await pilot.press("escape")
            await pilot.press("escape")
            await pilot.pause()
            self.assertEqual(app.view_mode, "failures")

    async def _assert_failure_selection_reconciles(
        self,
        app: BenchpressObserverApp,
        resolved_run_id: int,
        remaining_run_id: int,
    ) -> None:
        async with app.run_test() as pilot:
            await pilot.pause()
            app._execute_command(ObserverCommand("failures"))
            await pilot.pause()
            if app.selected_run_id != resolved_run_id:
                await pilot.press("down")
                await pilot.pause()
            self.assertEqual(app.selected_run_id, resolved_run_id)

            self.repo.update_run_status_phase(resolved_run_id, RunStatus.SUCCESS, RunPhase.DONE)
            app._refresh_snapshot("Manual refresh")
            await pilot.pause()

            self.assertEqual(app.view_mode, "failures")
            self.assertEqual(app.selected_run_id, remaining_run_id)
            self.assertEqual(app.selected_artifact_index, 0)

    def _session_config(self, refresh_seconds: float = 2.0):
        return _session_config_from_args(
            Namespace(
                db=str(self.db_path),
                spec=None,
                artifact_root=str(self.root),
                refresh_seconds=refresh_seconds,
            )
        )

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
            self._register_artifact(
                run,
                artifact_type="workload_output",
                artifact_name=artifact_name,
                artifact_text=artifact_text,
            )
        if error is not None:
            self._save_error(
                run,
                exception_type=error[0],
                message=error[1],
                phase=phase,
            )
        return run

    def _register_artifact(
        self,
        run: RunRecord,
        artifact_type: str,
        artifact_name: str,
        artifact_text: str | None = "artifact\n",
        artifact_path: Path | None = None,
        description: str = "artifact",
    ) -> Path:
        path = artifact_path or (run.output_dir / artifact_name)
        if artifact_text is not None:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(artifact_text, encoding="utf-8")
        self.repo.register_artifact(
            RunArtifact(
                run_id=run.run_id or 0,
                artifact_type=artifact_type,
                path=path,
                description=description,
            )
        )
        return path

    def _save_error(
        self,
        run: RunRecord,
        exception_type: str,
        message: str,
        phase: RunPhase,
    ) -> None:
        self.repo.save_error(
            ErrorRecord(
                run_id=run.run_id or 0,
                phase=phase,
                exception_type=exception_type,
                message=message,
            )
        )

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
