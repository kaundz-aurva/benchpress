from __future__ import annotations

from config.models import BenchmarkConfig
from orchestration.models import RunSpec, WorkloadProfile


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

