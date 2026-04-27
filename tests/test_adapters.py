from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from adapters.database.dto import SnapshotRequest
from adapters.database.sqlserver.service import SqlServerDatabaseAdapter
from adapters.host.windows.service import WindowsHostAdapter
from adapters.transport.dto import RemoteCommandRequest, RemoteCommandResult
from adapters.transport.service import TransportAdapter
from adapters.workload.hammerdb.service import HammerDBWorkloadRunner
from adapters.workload.dto import WorkloadExecutionRequest
from orchestration.models import AuditProfile, HostDefinition, HostRole, WorkloadProfile


class FakeTransport(TransportAdapter):
    def __init__(
        self,
        exit_code: int = 0,
        stdout: str = "tpm=100\nlatency_ms=12.5\nbenchmark_status=completed",
        stderr: str = "",
    ) -> None:
        self.exit_code = exit_code
        self.stdout = stdout
        self.stderr = stderr
        self.commands: list[str] = []
        self.environments: list[dict[str, str]] = []

    def execute_command(self, request: RemoteCommandRequest) -> RemoteCommandResult:
        self.commands.append(request.command)
        self.environments.append(dict(request.environment))
        return RemoteCommandResult(
            command=request.command,
            exit_code=self.exit_code,
            stdout=self.stdout,
            stderr=self.stderr or ("" if self.exit_code == 0 else "failed"),
        )

    def upload_file(self, local_path: Path, remote_path: Path) -> None:
        return None

    def download_file(self, remote_path: Path, local_path: Path) -> Path:
        return local_path

    def check_connectivity(self, host: HostDefinition) -> bool:
        return True


class AdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.target = HostDefinition("sql", HostRole.TARGET, "windows", "sql", 4, 16)
        self.client = HostDefinition("client", HostRole.CLIENT, "windows", "client", 2, 4)

    def test_sqlserver_adapter_validates_connectivity_with_transport(self) -> None:
        adapter = SqlServerDatabaseAdapter(
            host=self.target,
            connection_name="localhost",
            transport=FakeTransport(),
        )

        self.assertTrue(adapter.validate_connectivity())

    def test_sqlserver_audit_scripts_must_be_configured(self) -> None:
        adapter = SqlServerDatabaseAdapter(
            host=self.target,
            connection_name="localhost",
            transport=FakeTransport(),
        )
        with self.assertRaises(NotImplementedError):
            adapter.enable_audit(AuditProfile("audit_on", "audit_on"))

    def test_sqlserver_adapter_requires_transport(self) -> None:
        with self.assertRaises(ValueError):
            SqlServerDatabaseAdapter(
                host=self.target,
                connection_name="localhost",
                transport=None,
            )

    def test_sqlserver_snapshot_uses_output_file_command(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            transport = FakeTransport(stdout="")
            adapter = SqlServerDatabaseAdapter(
                host=self.target,
                connection_name="localhost",
                transport=transport,
                snapshot_query="SELECT name FROM sys.databases",
            )

            artifacts = adapter.capture_pre_snapshot(
                request=SnapshotRequest(
                    run_id=1,
                    host=self.target,
                    output_dir=Path(temp_dir),
                    label="pre",
                )
            )

            self.assertEqual(len(artifacts), 1)
            self.assertIn("-o", transport.commands[-1])

    def test_sqlserver_command_values_are_shell_quoted(self) -> None:
        transport = FakeTransport()
        adapter = SqlServerDatabaseAdapter(
            host=self.target,
            connection_name="localhost & echo bad",
            transport=transport,
            sqlcmd_path="sqlcmd & echo bad",
        )

        adapter.validate_connectivity()

        self.assertIn('"sqlcmd & echo bad"', transport.commands[-1])
        self.assertIn('"localhost & echo bad"', transport.commands[-1])

    def test_windows_host_adapter_validation_and_metadata(self) -> None:
        adapter = WindowsHostAdapter(host=self.target)
        metadata = adapter.collect_host_metadata()

        self.assertEqual(metadata["os_type"], "windows")
        with self.assertRaises(ValueError):
            WindowsHostAdapter(HostDefinition("linux", HostRole.TARGET, "linux", "h", 2, 4))

    def test_windows_metrics_require_configured_command(self) -> None:
        adapter = WindowsHostAdapter(host=self.target)
        with self.assertRaises(NotImplementedError):
            adapter.start_metrics_collection(1, Path("out"))

    def test_hammerdb_runner_executes_and_parses_results(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            transport = FakeTransport()
            runner = HammerDBWorkloadRunner(
                executable_path="hammerdb",
                transport=transport,
                script_path="run.tcl",
            )
            request = WorkloadExecutionRequest(
                run_id=1,
                workload_profile=WorkloadProfile("hammerdb_10vu"),
                target_host=self.target,
                client_host=self.client,
                audit_profile=AuditProfile("audit_off", "audit_off"),
                output_dir=Path(temp_dir),
            )

            runner.prepare_run(request)
            result = runner.execute_run(request)

            self.assertTrue(result.success)
            self.assertEqual(result.metrics["tpm"], 100)
            self.assertEqual(result.metrics["latency_ms"], 12.5)
            self.assertNotIn("--vu", transport.commands[-1])
            self.assertEqual(transport.environments[-1]["BENCHPRESS_VIRTUAL_USERS"], "10")

    def test_hammerdb_runner_passes_run_specific_environment(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            transport = FakeTransport()
            runner = HammerDBWorkloadRunner(
                executable_path="hammerdb",
                transport=transport,
                script_path="run.tcl",
            )
            request = WorkloadExecutionRequest(
                run_id=1,
                workload_profile=WorkloadProfile(
                    "hammerdb_11vu",
                    virtual_users=11,
                    warmup_minutes=2,
                    measured_minutes=3,
                    cooldown_minutes=4,
                ),
                target_host=self.target,
                client_host=self.client,
                audit_profile=AuditProfile("audit_off", "audit_off"),
                output_dir=Path(temp_dir),
            )

            runner.execute_run(request)

            self.assertEqual(
                transport.environments[-1],
                {
                    "BENCHPRESS_VIRTUAL_USERS": "11",
                    "BENCHPRESS_WARMUP_MINUTES": "2",
                    "BENCHPRESS_MEASURED_MINUTES": "3",
                    "BENCHPRESS_COOLDOWN_MINUTES": "4",
                },
            )

    def test_hammerdb_runner_normalizes_windows_script_paths(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            transport = FakeTransport()
            runner = HammerDBWorkloadRunner(
                executable_path="hammerdb",
                transport=transport,
                script_path=r"C:\benchpress\generated\hammerdb_tprocc_sqlserver.tcl",
            )
            request = WorkloadExecutionRequest(
                run_id=1,
                workload_profile=WorkloadProfile("hammerdb_10vu"),
                target_host=self.target,
                client_host=self.client,
                audit_profile=AuditProfile("audit_off", "audit_off"),
                output_dir=Path(temp_dir),
            )

            runner.execute_run(request)

            self.assertIn(
                '"C:/benchpress/generated/hammerdb_tprocc_sqlserver.tcl"',
                transport.commands[-1],
            )

    def test_hammerdb_runner_requires_transport_for_execution(self) -> None:
        runner = HammerDBWorkloadRunner(executable_path="hammerdb", script_path="run.tcl")
        request = WorkloadExecutionRequest(
            run_id=1,
            workload_profile=WorkloadProfile("hammerdb_10vu"),
            target_host=self.target,
            client_host=self.client,
            audit_profile=AuditProfile("audit_off", "audit_off"),
            output_dir=Path("out"),
        )

        with self.assertRaises(NotImplementedError):
            runner.execute_run(request)

    def test_hammerdb_parse_missing_file_raises(self) -> None:
        runner = HammerDBWorkloadRunner(executable_path="hammerdb")
        with self.assertRaises(FileNotFoundError):
            runner.parse_results(Path("missing.txt"))

    def test_hammerdb_runner_fails_on_usage_output(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runner = HammerDBWorkloadRunner(
                executable_path="hammerdb",
                transport=FakeTransport(stdout="Usage: hammerdb [ auto [ script_to_autoload.tcl ] ]"),
                script_path="run.tcl",
            )
            request = WorkloadExecutionRequest(
                run_id=1,
                workload_profile=WorkloadProfile("hammerdb_10vu"),
                target_host=self.target,
                client_host=self.client,
                audit_profile=AuditProfile("audit_off", "audit_off"),
                output_dir=Path(temp_dir),
            )

            result = runner.execute_run(request)

            self.assertFalse(result.success)
            self.assertIn("usage output", result.error_message.lower())

    def test_hammerdb_runner_requires_completion_marker(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            runner = HammerDBWorkloadRunner(
                executable_path="hammerdb",
                transport=FakeTransport(stdout="tpm=100\nlatency_ms=12.5"),
                script_path="run.tcl",
            )
            request = WorkloadExecutionRequest(
                run_id=1,
                workload_profile=WorkloadProfile("hammerdb_10vu"),
                target_host=self.target,
                client_host=self.client,
                audit_profile=AuditProfile("audit_off", "audit_off"),
                output_dir=Path(temp_dir),
            )

            result = runner.execute_run(request)

            self.assertFalse(result.success)
            self.assertIn("benchmark_status=completed", result.error_message)


if __name__ == "__main__":
    unittest.main()
