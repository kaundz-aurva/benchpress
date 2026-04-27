from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from generate_benchmark_assets import generate_assets_from_spec
from scripts.constants import (
    AUDIT_DISABLE_FILENAME,
    AUDIT_ENABLE_FILENAME,
    GENERATED_AGENT_CONFIG_FILENAME,
    HAMMERDB_TPROCC_FILENAME,
    METRICS_START_FILENAME,
    METRICS_STOP_FILENAME,
)


class AssetGenerationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.temp_dir.name)
        self.spec_path = self.root / "benchmark.json"
        self.spec_path.write_text(json.dumps(self._spec()), encoding="utf-8")

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_generator_writes_expected_files(self) -> None:
        output_dir = self.root / "generated"

        result = generate_assets_from_spec(self.spec_path, output_dir)

        self.assertEqual(Path(result["output_dir"]), output_dir)
        for filename in (
            AUDIT_ENABLE_FILENAME,
            AUDIT_DISABLE_FILENAME,
            "audit_snapshot_pre.sql",
            "audit_snapshot_post.sql",
            "audit_metadata.sql",
            HAMMERDB_TPROCC_FILENAME,
            METRICS_START_FILENAME,
            METRICS_STOP_FILENAME,
            GENERATED_AGENT_CONFIG_FILENAME,
        ):
            self.assertTrue((output_dir / filename).exists(), filename)

    def test_audit_enable_script_is_idempotent_and_uses_defaults(self) -> None:
        output_dir = self.root / "generated"
        generate_assets_from_spec(self.spec_path, output_dir)
        script = (output_dir / AUDIT_ENABLE_FILENAME).read_text(encoding="utf-8")

        self.assertIn("DROP SERVER AUDIT SPECIFICATION [Server-Audit-Spec-benchpress]", script)
        self.assertIn("DROP SERVER AUDIT [Audit-benchpress]", script)
        self.assertIn("CREATE SERVER AUDIT [Audit-benchpress]", script)
        self.assertIn("CREATE SERVER AUDIT SPECIFICATION [Server-Audit-Spec-benchpress]", script)
        self.assertIn("[Db-Audit-Spec-benchpress-master]", script)
        self.assertIn("[Db-Audit-Spec-benchpress-new]", script)
        self.assertIn("FILEPATH = N'D:\\rdsdbdata\\SQLAudit'", script)

    def test_audit_filter_and_disable_script_are_safe(self) -> None:
        output_dir = self.root / "generated"
        generate_assets_from_spec(self.spec_path, output_dir)
        enable_script = (output_dir / AUDIT_ENABLE_FILENAME).read_text(encoding="utf-8")
        disable_script = (output_dir / AUDIT_DISABLE_FILENAME).read_text(encoding="utf-8")

        self.assertIn("server_principal_name <> N'rdsa'", enable_script)
        self.assertIn("database_name <> N'msdb'", enable_script)
        self.assertIn("schema_name <> N'INFORMATION_SCHEMA'", enable_script)
        self.assertIn("WITH (STATE = OFF)", disable_script)
        self.assertNotIn("xp_cmdshell", disable_script.lower())
        self.assertNotIn("del ", disable_script.lower())
        self.assertNotIn("remove-item", disable_script.lower())

    def test_hammerdb_tcl_renders_tprocc_settings_and_markers(self) -> None:
        output_dir = self.root / "generated"
        generate_assets_from_spec(self.spec_path, output_dir)
        tcl = (output_dir / HAMMERDB_TPROCC_FILENAME).read_text(encoding="utf-8")

        self.assertIn("dbset db mssqls", tcl)
        self.assertIn("dbset bm TPC-C", tcl)
        self.assertIn("diset connection mssqls_server {sqlserver.internal}", tcl)
        self.assertIn("puts \"virtual_users=$virtual_users\"", tcl)
        self.assertIn("puts \"duration_seconds=", tcl)
        self.assertIn("puts \"benchmark_status=completed\"", tcl)

    def test_metrics_scripts_render_logman_and_artifact_markers(self) -> None:
        output_dir = self.root / "generated"
        generate_assets_from_spec(self.spec_path, output_dir)
        start_script = (output_dir / METRICS_START_FILENAME).read_text(encoding="utf-8")
        stop_script = (output_dir / METRICS_STOP_FILENAME).read_text(encoding="utf-8")

        self.assertIn("param(", start_script)
        self.assertIn("$RunRoot", start_script)
        self.assertIn("logman create counter", start_script)
        self.assertIn("logman start $CollectorName", start_script)
        self.assertIn("\\Processor(_Total)\\% Processor Time", start_script)
        self.assertIn("Invoke-BenchpressNative", start_script)
        self.assertIn("logman stop $CollectorName", stop_script)
        self.assertIn("host_metrics_csv", stop_script)
        self.assertIn("relog $($LatestBlg.FullName)", stop_script)
        self.assertIn("BENCHPRESS_ARTIFACT=", stop_script)

    def test_generated_agent_config_points_to_generated_files(self) -> None:
        output_dir = self.root / "generated"
        generate_assets_from_spec(self.spec_path, output_dir)
        config = json.loads((output_dir / GENERATED_AGENT_CONFIG_FILENAME).read_text(encoding="utf-8"))

        self.assertEqual(config["enable_audit_sql_file"], str(output_dir / AUDIT_ENABLE_FILENAME))
        self.assertEqual(config["disable_audit_sql_file"], str(output_dir / AUDIT_DISABLE_FILENAME))
        self.assertEqual(config["metrics_start_command"][-3], str(output_dir / METRICS_START_FILENAME))
        self.assertEqual(config["metrics_stop_command"][-3], str(output_dir / METRICS_STOP_FILENAME))
        self.assertEqual(config["metrics_start_command"][-2:], ["-RunId", "{run_id}"])
        self.assertEqual(config["metrics_stop_command"][-2:], ["-RunId", "{run_id}"])

    def test_invalid_asset_config_fails_validation(self) -> None:
        spec = self._spec()
        spec["assets"]["audit"]["audit_file_path"] = ""
        self.spec_path.write_text(json.dumps(spec), encoding="utf-8")

        with self.assertRaises(ValueError):
            generate_assets_from_spec(self.spec_path, self.root / "generated")

    def test_selected_databases_are_required(self) -> None:
        spec = self._spec()
        spec["assets"]["audit"]["selected_databases"] = []
        self.spec_path.write_text(json.dumps(spec), encoding="utf-8")

        with self.assertRaises(ValueError):
            generate_assets_from_spec(self.spec_path, self.root / "generated")

    def test_metrics_counters_are_required(self) -> None:
        spec = self._spec()
        spec["assets"]["metrics"]["counters"] = []
        self.spec_path.write_text(json.dumps(spec), encoding="utf-8")

        with self.assertRaises(ValueError):
            generate_assets_from_spec(self.spec_path, self.root / "generated")

    def _spec(self) -> dict[str, object]:
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
            "agent": {"base_url": "http://sql:8080"},
            "workload": {
                "hammerdb_executable_path": "C:/HammerDB/hammerdbcli.bat",
                "hammerdb_script_path": "C:/benchpress/generated/hammerdb_tprocc_sqlserver.tcl",
                "virtual_user_ladder": [10],
                "timings": {
                    "warmup_minutes": 1,
                    "measured_minutes": 2,
                    "cooldown_minutes": 1,
                },
                "repetitions": 1,
            },
            "storage": {
                "sqlite_path": str(self.root / "benchpress.sqlite3"),
                "output_root": str(self.root / "outputs"),
            },
            "assets": {
                "audit": {
                    "audit_file_path": "D:\\rdsdbdata\\SQLAudit",
                    "selected_databases": ["master", "new"],
                },
                "hammerdb": {
                    "sql_server": "sqlserver.internal",
                    "database_name": "tpcc",
                    "warehouses": 10,
                },
                "metrics": {
                    "collector_name": "BenchpressSqlMetrics",
                    "sample_interval_seconds": 5,
                    "output_root": "C:/benchpress/agent_artifacts/metrics",
                },
            },
        }


if __name__ == "__main__":
    unittest.main()
