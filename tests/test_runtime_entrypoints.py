from __future__ import annotations

import json
import os
import tempfile
import unittest
from pathlib import Path
from typing import Any

from adapters.workload.dto import WorkloadExecutionRequest, WorkloadExecutionResult
from adapters.workload.service import WorkloadRunner
from agents.sqlserver.dto import ArtifactInfo
from benchpress_orchestrator import run_benchmark_from_spec
from config.service import build_benchmark_config_from_runtime_spec, load_runtime_spec


class RuntimeEntrypointTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.spec_path = Path(self.temp_dir.name) / "benchmark.json"
        self.spec_path.write_text(json.dumps(self._spec()), encoding="utf-8")
        os.environ["BENCHPRESS_AGENT_TOKEN"] = "secret"

    def tearDown(self) -> None:
        os.environ.pop("BENCHPRESS_AGENT_TOKEN", None)
        self.temp_dir.cleanup()

    def test_runtime_spec_loads_and_builds_benchmark_config(self) -> None:
        spec = load_runtime_spec(self.spec_path)
        config = build_benchmark_config_from_runtime_spec(spec)

        self.assertEqual(config.virtual_user_ladder, (1,))
        self.assertEqual(config.repetitions, 1)
        self.assertEqual(config.audit_profiles[0].mode.value, "audit_off")

    def test_orchestrator_entrypoint_runs_with_fake_agent_and_workload(self) -> None:
        report = run_benchmark_from_spec(
            self.spec_path,
            agent_client_factory=lambda spec, token: FakeAgentClient(),
            workload_runner_factory=lambda spec: FakeWorkloadRunner(),
        )

        self.assertEqual(len(report["runs"]), 1)
        self.assertEqual(report["runs"][0]["status"], "success")
        self.assertTrue((Path(self.temp_dir.name) / "benchpress.sqlite3").exists())

    def test_missing_agent_token_fails_early(self) -> None:
        os.environ.pop("BENCHPRESS_AGENT_TOKEN", None)

        with self.assertRaises(ValueError):
            run_benchmark_from_spec(
                self.spec_path,
                agent_client_factory=lambda spec, token: FakeAgentClient(),
                workload_runner_factory=lambda spec: FakeWorkloadRunner(),
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
                "sqlite_path": str(Path(self.temp_dir.name) / "benchpress.sqlite3"),
                "output_root": str(Path(self.temp_dir.name) / "outputs"),
            },
        }


class FakeAgentClient:
    def health(self) -> dict[str, object]:
        return {"ok": True}

    def close(self) -> None:
        return None

    def enable_audit(self) -> dict[str, object]:
        return {"audit": "enabled"}

    def disable_audit(self) -> dict[str, object]:
        return {"audit": "disabled"}

    def validate_connectivity(self) -> dict[str, object]:
        return {"connected": True}

    def run_sanity_checks(self) -> dict[str, object]:
        return {"ok": True}

    def capture_snapshot(self, label: str, run_id: int) -> list[ArtifactInfo]:
        return [
            ArtifactInfo(
                artifact_id=1 if label == "pre" else 2,
                artifact_type=f"database_{label}_snapshot",
                path=f"{label}.txt",
            )
        ]

    def start_metrics_collection(self, run_id: int) -> dict[str, object]:
        return {"metrics": "started"}

    def stop_metrics_collection(self, run_id: int) -> list[ArtifactInfo]:
        return [
            ArtifactInfo(artifact_id=3, artifact_type="host_metrics", path="metrics.txt"),
            ArtifactInfo(artifact_id=4, artifact_type="host_metrics_csv", path="metrics.csv"),
        ]

    def collect_database_metadata(self) -> dict[str, object]:
        return {"engine": "sqlserver"}

    def collect_filesystem_stats(self) -> dict[str, object]:
        return {"free_bytes": 100}

    def collect_host_metadata(self) -> dict[str, object]:
        return {"os": "windows"}

    def download_artifact(self, artifact: ArtifactInfo, destination_dir: Path) -> Path:
        destination_dir.mkdir(parents=True, exist_ok=True)
        path = destination_dir / Path(artifact.path).name
        path.write_text(artifact.artifact_type, encoding="utf-8")
        return path


class FakeWorkloadRunner(WorkloadRunner):
    def prepare_run(self, request: WorkloadExecutionRequest) -> None:
        request.output_dir.mkdir(parents=True, exist_ok=True)

    def execute_run(self, request: WorkloadExecutionRequest) -> WorkloadExecutionResult:
        path = request.output_dir / "workload.txt"
        path.write_text("workload", encoding="utf-8")
        return WorkloadExecutionResult(
            success=True,
            artifacts=(path,),
            metrics={"tpm": 1},
            raw_output_path=path,
        )

    def parse_results(self, result_path: Path) -> dict[str, Any]:
        return {"tpm": 1}


if __name__ == "__main__":
    unittest.main()
