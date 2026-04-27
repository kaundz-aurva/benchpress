from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from typing import Sequence

from fastapi.testclient import TestClient

from agents.sqlserver.app import create_app
from agents.sqlserver.models import LocalCommandResult, SqlServerAgentConfig
from agents.sqlserver.service import SqlServerAgentService


class SqlServerAgentAppTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.commands: list[Sequence[str]] = []

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_health_and_authentication(self) -> None:
        client = TestClient(create_app(self._service(), bearer_token="secret"))

        self.assertEqual(client.get("/health").status_code, 200)
        self.assertEqual(client.post("/audit/enable").status_code, 401)
        response = client.post("/audit/enable", headers=self._auth())

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["details"]["audit"], "enabled")

    def test_snapshot_registers_and_downloads_artifact(self) -> None:
        client = TestClient(create_app(self._service(), bearer_token="secret"))
        response = client.post("/snapshots/pre", json={"run_id": 1}, headers=self._auth())
        artifact = response.json()["artifacts"][0]
        download = client.get(f"/artifacts/{artifact['artifact_id']}", headers=self._auth())

        self.assertEqual(response.status_code, 200)
        self.assertEqual(artifact["artifact_type"], "database_pre_snapshot")
        self.assertEqual(download.status_code, 200)
        self.assertEqual(download.content, b"snapshot output")

    def test_metrics_commands_substitute_run_id(self) -> None:
        client = TestClient(create_app(self._service(), bearer_token="secret"))

        start_response = client.post("/metrics/start", json={"run_id": 7}, headers=self._auth())
        stop_response = client.post("/metrics/stop", json={"run_id": 7}, headers=self._auth())

        self.assertEqual(start_response.status_code, 200)
        self.assertEqual(stop_response.status_code, 200)
        self.assertIn(("powershell", "start", "7"), self.commands)
        self.assertIn(("powershell", "stop", "7"), self.commands)

    def test_agent_errors_are_sanitized(self) -> None:
        def failing_runner(command: Sequence[str], timeout: int) -> LocalCommandResult:
            return LocalCommandResult(
                command=" ".join(command),
                exit_code=1,
                stderr="secret database detail",
            )

        client = TestClient(
            create_app(
                SqlServerAgentService(self._config(), command_runner=failing_runner),
                bearer_token="secret",
            )
        )

        response = client.post("/database/connectivity", headers=self._auth())

        self.assertEqual(response.status_code, 400)
        self.assertEqual(response.json()["detail"], "SQL command failed")
        self.assertNotIn("secret", response.text)

    def test_missing_artifact_returns_404(self) -> None:
        client = TestClient(create_app(self._service(), bearer_token="secret"))

        response = client.get("/artifacts/999", headers=self._auth())

        self.assertEqual(response.status_code, 404)

    def _service(self) -> SqlServerAgentService:
        return SqlServerAgentService(self._config(), command_runner=self._runner)

    def _config(self) -> SqlServerAgentConfig:
        return SqlServerAgentConfig(
            sql_connection_name="localhost",
            staging_root=Path(self.temp_dir.name),
            enable_audit_sql="ENABLE AUDIT",
            disable_audit_sql="DISABLE AUDIT",
            metrics_start_command=("powershell", "start", "{run_id}"),
            metrics_stop_command=("powershell", "stop", "{run_id}"),
        )

    def _runner(self, command: Sequence[str], timeout: int) -> LocalCommandResult:
        self.commands.append(command)
        if "-o" in command:
            output_path = Path(command[command.index("-o") + 1])
            output_path.write_text("snapshot output", encoding="utf-8")
        return LocalCommandResult(command=" ".join(command), exit_code=0, stdout="metrics")

    def _auth(self) -> dict[str, str]:
        return {"Authorization": "Bearer secret"}


if __name__ == "__main__":
    unittest.main()
