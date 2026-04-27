from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import httpx

from adapters.database.dto import SnapshotRequest
from adapters.database.sqlserver_agent.service import SqlServerAgentDatabaseAdapter
from adapters.host.windows_agent.service import WindowsAgentHostAdapter
from agents.sqlserver.client import SqlServerAgentClient
from orchestration.models import AuditProfile, HostDefinition, HostRole


class AgentClientAdapterTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.client = SqlServerAgentClient(
            base_url="http://agent",
            bearer_token="secret",
            transport=httpx.MockTransport(self._handler),
        )
        self.host = HostDefinition("sql", HostRole.TARGET, "windows", "sql", 4, 16)

    def tearDown(self) -> None:
        self.client.close()
        self.temp_dir.cleanup()

    def test_database_adapter_uses_agent_client_and_downloads_snapshot(self) -> None:
        adapter = SqlServerAgentDatabaseAdapter(self.client)
        request = SnapshotRequest(
            run_id=1,
            host=self.host,
            output_dir=Path(self.temp_dir.name),
            label="pre",
        )

        self.assertTrue(adapter.validate_connectivity())
        adapter.disable_audit(AuditProfile("off", "audit_off"))
        artifacts = adapter.capture_pre_snapshot(request)

        self.assertEqual(len(artifacts), 1)
        self.assertEqual(artifacts[0].path.read_text(encoding="utf-8"), "snapshot")
        self.assertEqual(adapter.collect_database_metadata()["engine"], "sqlserver")

    def test_host_adapter_uses_agent_client_and_downloads_metrics(self) -> None:
        adapter = WindowsAgentHostAdapter(self.client)

        adapter.start_metrics_collection(1, Path(self.temp_dir.name))
        artifacts = adapter.stop_metrics_collection(1, Path(self.temp_dir.name))

        self.assertEqual(len(artifacts), 2)
        self.assertEqual(artifacts[0].path.read_text(encoding="utf-8"), "metrics")
        self.assertEqual(adapter.collect_host_metadata()["os"], "windows")
        self.assertEqual(adapter.collect_filesystem_stats()["free_bytes"], 100)

    def test_host_adapter_raises_when_csv_artifact_is_missing(self) -> None:
        client = SqlServerAgentClient(
            base_url="http://agent",
            bearer_token="secret",
            transport=httpx.MockTransport(self._handler_without_csv),
        )
        adapter = WindowsAgentHostAdapter(client)

        with self.assertRaises(RuntimeError):
            adapter.stop_metrics_collection(1, Path(self.temp_dir.name))

        client.close()

    def test_client_raises_on_agent_error(self) -> None:
        client = SqlServerAgentClient(
            base_url="http://agent",
            bearer_token="secret",
            transport=httpx.MockTransport(
                lambda request: httpx.Response(400, json={"detail": "bad request"})
            ),
        )

        with self.assertRaises(RuntimeError):
            client.enable_audit()

        client.close()

    def _handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/health":
            return self._json({"ok": True, "service": "benchpress-sqlserver-agent"})
        if path in {"/audit/enable", "/audit/disable"}:
            return self._json({"ok": True, "details": {"audit": "ok"}})
        if path == "/database/connectivity":
            return self._json({"ok": True, "details": {"connected": True}})
        if path == "/database/sanity":
            return self._json({"metadata": {"ok": True}})
        if path == "/metadata/database":
            return self._json({"metadata": {"engine": "sqlserver"}})
        if path == "/metadata/host":
            return self._json({"metadata": {"os": "windows"}})
        if path == "/metadata/filesystem":
            return self._json({"metadata": {"free_bytes": 100}})
        if path == "/metrics/start":
            return self._json({"ok": True, "details": {"metrics": "started"}})
        if path == "/metrics/stop":
            return self._json(
                {
                    "artifacts": [
                        {
                            "artifact_id": 2,
                            "artifact_type": "host_metrics",
                            "path": "windows_metrics_stop.txt",
                        },
                        {
                            "artifact_id": 3,
                            "artifact_type": "host_metrics_csv",
                            "path": "metrics.csv",
                        }
                    ]
                }
            )
        if path == "/snapshots/pre":
            return self._json(
                {
                    "artifacts": [
                        {
                            "artifact_id": 1,
                            "artifact_type": "database_pre_snapshot",
                            "path": "sqlserver_pre_snapshot.txt",
                        }
                    ]
                }
            )
        if path == "/artifacts/1":
            return httpx.Response(200, content=b"snapshot")
        if path == "/artifacts/2":
            return httpx.Response(200, content=b"metrics")
        if path == "/artifacts/3":
            return httpx.Response(200, content=b"(PDH-CSV 4.0)\n")
        return httpx.Response(404, json={"detail": f"unknown path {path}"})

    def _handler_without_csv(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path == "/metrics/stop":
            return self._json(
                {
                    "artifacts": [
                        {
                            "artifact_id": 2,
                            "artifact_type": "host_metrics",
                            "path": "windows_metrics_stop.txt",
                        }
                    ]
                }
            )
        if path == "/artifacts/2":
            return httpx.Response(
                200,
                content=b"BENCHPRESS_ARTIFACT=C:/metrics.blg|host_metrics_blg|Windows PerfMon BLG metrics",
            )
        return self._handler(request)

    def _json(self, data: dict[str, object]) -> httpx.Response:
        return httpx.Response(200, content=json.dumps(data).encode("utf-8"))


if __name__ == "__main__":
    unittest.main()
