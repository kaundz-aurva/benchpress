from __future__ import annotations

import csv
import io
import json
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from typing import Any

from db.repository import BenchmarkRepository
from generate_benchmark_report import generate_report_from_db, main as report_main
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
from reporting.repository import ReportingRepository


class ReportingTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.db_path = self.root / "benchpress.sqlite3"
        self.repo = BenchmarkRepository(self.db_path)
        self.repo.create_schema()

    def tearDown(self) -> None:
        self.repo.close()
        self.temp_dir.cleanup()

    def test_repository_reads_joined_runs_with_artifacts_and_errors(self) -> None:
        run = self._seed_run(
            audit_mode="audit_off",
            virtual_users=10,
            metrics={"tpm": 100},
            status=RunStatus.FAILED,
            phase=RunPhase.METRICS_START,
            error=("TimeoutError", "timed out"),
            artifact_text="tpm=100\n",
        )

        with ReportingRepository(self.db_path) as repository:
            rows = repository.list_runs()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0].run_id, run.run_id)
        self.assertEqual(rows[0].audit_mode, "audit_off")
        self.assertEqual(rows[0].virtual_users, 10)
        self.assertEqual(rows[0].target_memory_gb, 16)
        self.assertEqual(rows[0].summary_metrics["workload"]["tpm"], 100)
        self.assertEqual(len(rows[0].artifacts), 1)
        self.assertEqual(rows[0].errors[0].phase, "metrics_start")

    def test_report_generates_markdown_and_csv_comparison(self) -> None:
        self._seed_run("audit_off", 10, repetition=1, metrics={"tpm": 100, "nopm": 80})
        self._seed_run("audit_off", 10, repetition=2, metrics={"tpm": 110, "nopm": 88})
        self._seed_run("audit_on", 10, repetition=1, metrics={"tpm": 90, "nopm": 72})
        self._seed_run("audit_on", 10, repetition=2, metrics={"tpm": 95, "nopm": 76})
        markdown_path = self.root / "reports" / "summary.md"

        result = generate_report_from_db(self.db_path, markdown_path)

        self.assertEqual(result["total_runs"], 4)
        self.assertTrue(markdown_path.exists())
        markdown = markdown_path.read_text(encoding="utf-8")
        self.assertIn("Per-VU Throughput Comparison", markdown)
        self.assertIn("audit_on throughput was lower", markdown)

        overhead_rows = self._csv_rows(self.root / "reports" / "csv" / "overhead.csv")
        self.assertEqual(len(overhead_rows), 1)
        self.assertEqual(overhead_rows[0]["virtual_users"], "10")
        self.assertAlmostEqual(float(overhead_rows[0]["tpm_audit_off_mean"]), 105.0)
        self.assertAlmostEqual(float(overhead_rows[0]["tpm_audit_on_mean"]), 92.5)
        self.assertLess(float(overhead_rows[0]["tpm_percent_change"]), 0)

    def test_failed_runs_are_excluded_from_aggregates_and_listed(self) -> None:
        self._seed_run("audit_off", 20, metrics={"tpm": 200})
        failed_run = self._seed_run(
            "audit_on",
            20,
            metrics={"tpm": 1},
            status=RunStatus.FAILED,
            phase=RunPhase.METRICS_START,
            error=("TimeoutError", "run timed out at metrics_start"),
        )
        markdown_path = self.root / "report.md"

        generate_report_from_db(self.db_path, markdown_path)

        aggregate_rows = self._csv_rows(self.root / "csv" / "aggregates.csv")
        self.assertEqual(len(aggregate_rows), 1)
        self.assertEqual(aggregate_rows[0]["audit_mode"], "audit_off")

        failure_rows = self._csv_rows(self.root / "csv" / "failures.csv")
        self.assertEqual(failure_rows[0]["run_id"], str(failed_run.run_id))
        self.assertEqual(failure_rows[0]["phase"], "metrics_start")
        self.assertEqual(failure_rows[0]["exception_type"], "TimeoutError")
        self.assertIn("run timed out", markdown_path.read_text(encoding="utf-8"))

    def test_missing_summaries_and_missing_throughput_do_not_fail(self) -> None:
        self._seed_run("audit_off", 30, metrics=None)
        markdown_path = self.root / "reports" / "missing.md"

        generate_report_from_db(self.db_path, markdown_path)

        markdown = markdown_path.read_text(encoding="utf-8")
        self.assertIn("not available", markdown)
        run_rows = self._csv_rows(self.root / "reports" / "csv" / "runs.csv")
        self.assertEqual(run_rows[0].get("tpm", ""), "")

    def test_artifact_fallback_parses_hammerdb_stdout_metrics(self) -> None:
        self._seed_run(
            "audit_off",
            40,
            metrics=None,
            artifact_text=(
                "tpm=42\n"
                "nopm=39\n"
                "custom_metric=7\n"
                "benchmark_status=completed\n"
            ),
        )
        markdown_path = self.root / "fallback.md"

        generate_report_from_db(self.db_path, markdown_path)

        run_rows = self._csv_rows(self.root / "csv" / "runs.csv")
        self.assertEqual(run_rows[0]["tpm"], "42")
        self.assertEqual(run_rows[0]["nopm"], "39")
        self.assertEqual(run_rows[0]["custom_metric"], "7")
        self.assertEqual(run_rows[0]["benchmark_status"], "completed")

    def test_host_metrics_are_summarized_averaged_and_rendered_in_html(self) -> None:
        self._seed_run(
            "audit_off",
            60,
            repetition=1,
            metrics={"tpm": 100},
            host_metric_csv=self._perfmon_csv(
                total_cpu=(10, 20),
                available_memory=(12000, 11000),
                sql_cpu=(30, 40),
                working_set=(104857600, 209715200),
            ),
        )
        self._seed_run(
            "audit_off",
            60,
            repetition=2,
            metrics={"tpm": 110},
            host_metric_csv=self._perfmon_csv(
                total_cpu=(20, 30),
                available_memory=(11800, 10800),
                sql_cpu=(35, 45),
                working_set=(209715200, 314572800),
            ),
        )
        self._seed_run(
            "audit_on",
            60,
            repetition=1,
            metrics={"tpm": 90},
            host_metric_csv=self._perfmon_csv(
                total_cpu=(40, 50),
                available_memory=(10000, 9000),
                sql_cpu=(50, 60),
                working_set=(314572800, 419430400),
            ),
        )
        self._seed_run(
            "audit_on",
            60,
            repetition=2,
            metrics={"tpm": 95},
            host_metric_csv=self._perfmon_csv(
                total_cpu=(50, 60),
                available_memory=(9800, 8800),
                sql_cpu=(55, 65),
                working_set=(419430400, 524288000),
            ),
        )
        markdown_path = self.root / "host" / "summary.md"
        html_path = self.root / "host" / "summary.html"

        result = generate_report_from_db(self.db_path, markdown_path)

        self.assertEqual(result["html_path"], str(html_path))
        self.assertTrue(html_path.exists())
        html = html_path.read_text(encoding="utf-8")
        self.assertIn("CPU and Memory Metrics", html)
        self.assertIn("<svg", html)
        self.assertIn("Run 1 CPU", html)

        host_runs = self._csv_rows(self.root / "host" / "csv" / "host_runs.csv")
        self.assertEqual(host_runs[0]["sample_count"], "2")
        self.assertAlmostEqual(float(host_runs[0]["total_cpu_percent_avg"]), 15.0)
        self.assertAlmostEqual(float(host_runs[0]["sql_working_set_mb_avg"]), 150.0)
        self.assertGreater(float(host_runs[0]["memory_used_percent_avg"]), 0.0)

        host_overhead = self._csv_rows(self.root / "host" / "csv" / "host_overhead.csv")
        self.assertEqual(host_overhead[0]["virtual_users"], "60")
        self.assertAlmostEqual(float(host_overhead[0]["total_cpu_percent_avg_audit_off_mean"]), 20.0)
        self.assertAlmostEqual(float(host_overhead[0]["total_cpu_percent_avg_audit_on_mean"]), 50.0)
        self.assertAlmostEqual(float(host_overhead[0]["total_cpu_percent_avg_delta"]), 30.0)

        host_samples = self._csv_rows(self.root / "host" / "csv" / "host_samples.csv")
        self.assertEqual(len(host_samples), 8)
        self.assertEqual(host_samples[0]["timestamp"], "2026-04-23 10:00:00.000")
        self.assertTrue((self.root / "host" / "csv" / "host_metrics_cache.json").exists())

    def test_html_out_cli_argument_is_respected(self) -> None:
        self._seed_run("audit_off", 70, metrics={"tpm": 700})
        markdown_path = self.root / "cli-html" / "summary.md"
        html_path = self.root / "custom" / "report.html"
        stdout = io.StringIO()

        with redirect_stdout(stdout):
            exit_code = report_main(
                [
                    "--db",
                    str(self.db_path),
                    "--out",
                    str(markdown_path),
                    "--html-out",
                    str(html_path),
                ]
            )

        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["html_path"], str(html_path))
        self.assertTrue(html_path.exists())

    def test_cli_entrypoint_writes_outputs(self) -> None:
        self._seed_run("audit_off", 50, metrics={"tpm": 500})
        markdown_path = self.root / "cli" / "summary.md"
        stdout = io.StringIO()

        with redirect_stdout(stdout):
            exit_code = report_main(["--db", str(self.db_path), "--out", str(markdown_path)])

        self.assertEqual(exit_code, 0)
        payload = json.loads(stdout.getvalue())
        self.assertEqual(payload["markdown_path"], str(markdown_path))
        self.assertTrue(markdown_path.exists())
        self.assertTrue((self.root / "cli" / "summary.html").exists())
        self.assertTrue((self.root / "cli" / "csv" / "runs.csv").exists())

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
        host_metric_csv: str | None = None,
    ) -> RunRecord:
        benchmark = self.repo.create_benchmark_profile(BenchmarkProfile("bench"))
        target = self.repo.create_host(
            HostDefinition("sql", HostRole.TARGET, "windows", "sql", 4, 16)
        )
        client = self.repo.create_host(
            HostDefinition("client", HostRole.CLIENT, "windows", "client", 2, 4)
        )
        workload = self.repo.create_workload_profile(
            WorkloadProfile(
                name=f"hammerdb_{virtual_users}vu",
                virtual_users=virtual_users,
            )
        )
        audit = self.repo.create_audit_profile(AuditProfile(audit_mode, audit_mode))
        output_dir = self.root / "outputs" / audit_mode / f"{virtual_users}vu" / f"rep{repetition}"
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
            )
        )
        run_id = run.run_id or 0
        if metrics is not None:
            self.repo.save_summary(RunSummary(run_id=run_id, metrics={"workload": metrics}))
        if artifact_text is not None:
            artifact_path = output_dir / "hammerdb_stdout.txt"
            artifact_path.write_text(artifact_text, encoding="utf-8")
            self.repo.register_artifact(
                RunArtifact(
                    run_id=run_id,
                    artifact_type="workload_output",
                    path=artifact_path,
                    description="HammerDB stdout",
                )
            )
        if host_metric_csv is not None:
            host_metric_path = output_dir / "benchpress_metrics.csv"
            host_metric_path.write_text(host_metric_csv, encoding="utf-8")
            self.repo.register_artifact(
                RunArtifact(
                    run_id=run_id,
                    artifact_type="host_metrics_csv",
                    path=host_metric_path,
                    description="Windows PerfMon CSV metrics",
                )
            )
        if error is not None:
            exception_type, message = error
            self.repo.save_error(
                ErrorRecord(
                    run_id=run_id,
                    phase=phase,
                    exception_type=exception_type,
                    message=message,
                )
            )
        return self.repo.update_run_status_phase(run_id, status=status, phase=phase)

    def _csv_rows(self, path: Path) -> list[dict[str, str]]:
        with path.open("r", encoding="utf-8", newline="") as file_obj:
            return list(csv.DictReader(file_obj))

    def _perfmon_csv(
        self,
        total_cpu: tuple[int, int],
        available_memory: tuple[int, int],
        sql_cpu: tuple[int, int],
        working_set: tuple[int, int],
    ) -> str:
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(
            [
                "(PDH-CSV 4.0) (UTC)(0)",
                "\\\\SQL\\Processor(_Total)\\% Processor Time",
                "\\\\SQL\\Memory\\Available MBytes",
                "\\\\SQL\\Process(sqlservr)\\% Processor Time",
                "\\\\SQL\\Process(sqlservr)\\Working Set",
            ]
        )
        writer.writerow(
            [
                "2026-04-23 10:00:00.000",
                total_cpu[0],
                available_memory[0],
                sql_cpu[0],
                working_set[0],
            ]
        )
        writer.writerow(
            [
                "2026-04-23 10:00:05.000",
                total_cpu[1],
                available_memory[1],
                sql_cpu[1],
                working_set[1],
            ]
        )
        return output.getvalue()


if __name__ == "__main__":
    unittest.main()
