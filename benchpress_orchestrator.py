from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Callable, Protocol

from adapters.database.sqlserver_agent.service import SqlServerAgentDatabaseAdapter
from adapters.host.windows_agent.service import WindowsAgentHostAdapter
from adapters.transport.local.service import LocalTransport
from adapters.workload.hammerdb.service import HammerDBWorkloadRunner
from adapters.workload.service import WorkloadRunner
from agents.sqlserver.client import SqlServerAgentClient
from config.dto import BenchmarkRuntimeSpecDto
from config.service import build_benchmark_config_from_runtime_spec, load_runtime_spec
from db.repository import BenchmarkRepository
from orchestration.models import RunRecord
from orchestration.service import BenchmarkOrchestrationService


class AgentClientProtocol(Protocol):
    def health(self) -> dict[str, object]:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError


AgentClientFactory = Callable[[BenchmarkRuntimeSpecDto, str], SqlServerAgentClient]
WorkloadRunnerFactory = Callable[[BenchmarkRuntimeSpecDto], WorkloadRunner]


def run_benchmark_from_spec(
    spec_path: Path | str,
    agent_client_factory: AgentClientFactory | None = None,
    workload_runner_factory: WorkloadRunnerFactory | None = None,
) -> dict[str, object]:
    spec = load_runtime_spec(spec_path)
    token = os.environ.get(spec.agent.bearer_token_env, "")
    if not token.strip():
        raise ValueError(f"{spec.agent.bearer_token_env} is required")

    spec.storage.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
    spec.storage.output_root.mkdir(parents=True, exist_ok=True)

    repository = BenchmarkRepository(spec.storage.sqlite_path)
    repository.create_schema()
    agent_client = (
        agent_client_factory(spec, token)
        if agent_client_factory is not None
        else _default_agent_client_factory(spec, token)
    )
    try:
        agent_client.health()
        database_adapter = SqlServerAgentDatabaseAdapter(agent_client)
        host_adapter = WindowsAgentHostAdapter(agent_client)
        workload_runner = (
            workload_runner_factory(spec)
            if workload_runner_factory is not None
            else _default_workload_runner_factory(spec)
        )
        orchestration = BenchmarkOrchestrationService(
            repository=repository,
            database_adapter=database_adapter,
            target_host_adapter=host_adapter,
            workload_runner=workload_runner,
        )
        benchmark_config = build_benchmark_config_from_runtime_spec(spec)
        records = [
            orchestration.execute_single_run(run_spec)
            for run_spec in orchestration.build_run_matrix(benchmark_config)
        ]
        return _execution_report(records, spec.storage.sqlite_path, spec.storage.output_root)
    finally:
        agent_client.close()
        repository.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run a Benchpress benchmark from a JSON spec.")
    parser.add_argument("--spec", required=True, help="Path to benchmark runtime JSON spec.")
    args = parser.parse_args(argv)
    report = run_benchmark_from_spec(args.spec)
    print(json.dumps(report, indent=2, sort_keys=True))
    failed_runs = [
        run
        for run in report["runs"]
        if isinstance(run, dict) and run.get("status") != "success"
    ]
    return 1 if failed_runs else 0


def _default_agent_client_factory(
    spec: BenchmarkRuntimeSpecDto,
    token: str,
) -> SqlServerAgentClient:
    return SqlServerAgentClient(
        base_url=spec.agent.base_url,
        bearer_token=token,
        timeout_seconds=spec.agent.timeout_seconds,
    )


def _default_workload_runner_factory(spec: BenchmarkRuntimeSpecDto) -> WorkloadRunner:
    return HammerDBWorkloadRunner(
        executable_path=spec.workload.hammerdb_executable_path,
        transport=LocalTransport(),
        script_path=spec.workload.hammerdb_script_path,
        result_filename=spec.workload.result_filename,
    )


def _execution_report(
    records: list[RunRecord],
    sqlite_path: Path,
    output_root: Path,
) -> dict[str, object]:
    return {
        "sqlite_path": str(sqlite_path),
        "output_root": str(output_root),
        "runs": [
            {
                "run_id": record.run_id,
                "status": record.status.value,
                "phase": record.phase.value,
                "output_dir": str(record.output_dir),
            }
            for record in records
        ],
    }


if __name__ == "__main__":
    raise SystemExit(main())

