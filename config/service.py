from __future__ import annotations

import json
from pathlib import Path

from config.dto import BenchmarkRuntimeSpecDto
from config.models import BenchmarkConfig, RunTimingConfig
from orchestration.models import (
    AuditProfile,
    BenchmarkProfile,
    HostDefinition,
    RunSpec,
    WorkloadProfile,
)


class BenchmarkConfigService:
    def build_run_matrix(self, config: BenchmarkConfig) -> list[RunSpec]:
        specs: list[RunSpec] = []
        for audit_profile in config.audit_profiles:
            for virtual_users in config.virtual_user_ladder:
                workload_profile = WorkloadProfile(
                    name=f"{config.workload_tool}_{virtual_users}vu",
                    tool=config.workload_tool,
                    virtual_users=virtual_users,
                    warmup_minutes=config.timings.warmup_minutes,
                    measured_minutes=config.timings.measured_minutes,
                    cooldown_minutes=config.timings.cooldown_minutes,
                    metadata=config.workload_metadata,
                )
                for repetition in range(1, config.repetitions + 1):
                    output_root = (
                        config.output_root
                        / audit_profile.mode.value
                        / f"{virtual_users}vu"
                        / f"rep_{repetition}"
                    )
                    specs.append(
                        RunSpec(
                            benchmark_profile=config.benchmark_profile,
                            target_host=config.target_host,
                            client_host=config.client_host,
                            workload_profile=workload_profile,
                            audit_profile=audit_profile,
                            repetition=repetition,
                            output_root=output_root,
                        )
                    )
        return specs


def load_runtime_spec(path: Path | str) -> BenchmarkRuntimeSpecDto:
    spec_path = Path(path)
    data = json.loads(spec_path.read_text(encoding="utf-8"))
    return BenchmarkRuntimeSpecDto.model_validate(data)


def build_benchmark_config_from_runtime_spec(spec: BenchmarkRuntimeSpecDto) -> BenchmarkConfig:
    benchmark_profile = BenchmarkProfile(
        name=spec.benchmark_profile.name,
        database_engine=spec.benchmark_profile.database_engine,
        database_version=spec.benchmark_profile.database_version,
        cloud_provider=spec.benchmark_profile.cloud_provider,
        description=spec.benchmark_profile.description,
    )
    target_host = HostDefinition(
        name=spec.target_host.name,
        role=spec.target_host.role,
        os_type=spec.target_host.os_type,
        hostname=spec.target_host.hostname,
        vcpus=spec.target_host.vcpus,
        memory_gb=spec.target_host.memory_gb,
        cloud_instance_id=spec.target_host.cloud_instance_id,
    )
    client_host = HostDefinition(
        name=spec.client_host.name,
        role=spec.client_host.role,
        os_type=spec.client_host.os_type,
        hostname=spec.client_host.hostname,
        vcpus=spec.client_host.vcpus,
        memory_gb=spec.client_host.memory_gb,
        cloud_instance_id=spec.client_host.cloud_instance_id,
    )
    return BenchmarkConfig(
        benchmark_profile=benchmark_profile,
        target_host=target_host,
        client_host=client_host,
        audit_profiles=tuple(
            AuditProfile(name=mode, mode=mode)
            for mode in spec.audit.modes
        ),
        virtual_user_ladder=tuple(spec.workload.virtual_user_ladder),
        repetitions=spec.workload.repetitions,
        timings=RunTimingConfig(
            warmup_minutes=spec.workload.timings.warmup_minutes,
            measured_minutes=spec.workload.timings.measured_minutes,
            cooldown_minutes=spec.workload.timings.cooldown_minutes,
        ),
        output_root=spec.storage.output_root,
        workload_tool=spec.workload.tool,
    )
