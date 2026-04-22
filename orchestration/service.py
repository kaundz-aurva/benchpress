from __future__ import annotations

from adapters.database.dto import SnapshotRequest
from adapters.database.service import DatabaseAdapter
from adapters.host.service import HostAdapter
from adapters.workload.dto import WorkloadExecutionRequest, WorkloadExecutionResult
from adapters.workload.service import WorkloadRunner
from config.models import BenchmarkConfig
from config.service import BenchmarkConfigService
from db.repository import BenchmarkRepository
from orchestration.models import (
    AuditMode,
    ErrorRecord,
    RunArtifact,
    RunRecord,
    RunSpec,
    RunStatus,
    RunPhase,
    RunSummary,
)


class BenchmarkOrchestrationService:
    def __init__(
        self,
        repository: BenchmarkRepository,
        database_adapter: DatabaseAdapter,
        target_host_adapter: HostAdapter,
        workload_runner: WorkloadRunner,
        config_service: BenchmarkConfigService | None = None,
    ) -> None:
        self.repository = repository
        self.database_adapter = database_adapter
        self.target_host_adapter = target_host_adapter
        self.workload_runner = workload_runner
        self.config_service = config_service or BenchmarkConfigService()

    def build_run_matrix(self, config: BenchmarkConfig) -> list[RunSpec]:
        return self.config_service.build_run_matrix(config)

    def create_run_record(self, spec: RunSpec) -> RunRecord:
        benchmark_profile = self.repository.create_benchmark_profile(spec.benchmark_profile)
        target_host = self.repository.create_host(spec.target_host)
        client_host = self.repository.create_host(spec.client_host)
        workload_profile = self.repository.create_workload_profile(spec.workload_profile)
        audit_profile = self.repository.create_audit_profile(spec.audit_profile)
        spec.output_root.mkdir(parents=True, exist_ok=True)
        return self.repository.create_run(
            RunRecord(
                benchmark_profile_id=self._required_id(
                    benchmark_profile.profile_id,
                    "benchmark_profile_id",
                ),
                target_host_id=self._required_id(target_host.host_id, "target_host_id"),
                client_host_id=self._required_id(client_host.host_id, "client_host_id"),
                workload_profile_id=self._required_id(
                    workload_profile.workload_profile_id,
                    "workload_profile_id",
                ),
                audit_profile_id=self._required_id(audit_profile.audit_profile_id, "audit_profile_id"),
                repetition=spec.repetition,
                output_dir=spec.output_root,
            )
        )

    def execute_single_run(self, spec: RunSpec) -> RunRecord:
        run = self.create_run_record(spec)
        current_phase = RunPhase.SETUP
        metrics_started = False
        try:
            run = self._transition(run, RunStatus.RUNNING, RunPhase.SETUP)
            self._configure_audit(spec)

            current_phase = RunPhase.PRECHECK
            run = self._transition(run, phase=current_phase)
            sanity = self._run_precheck()

            current_phase = RunPhase.METRICS_START
            run = self._transition(run, phase=current_phase)
            self._start_metrics(run)
            metrics_started = True

            current_phase = RunPhase.PRE_SNAPSHOT
            run = self._transition(run, phase=current_phase)
            self._capture_pre_snapshot(run, spec)

            current_phase = RunPhase.WORKLOAD_RUN
            run = self._transition(run, phase=current_phase)
            workload_result = self._execute_workload(run, spec)

            current_phase = RunPhase.POST_SNAPSHOT
            run = self._transition(run, phase=current_phase)
            self._capture_post_snapshot(run, spec)

            current_phase = RunPhase.METRICS_STOP
            run = self._transition(run, phase=current_phase)
            self._stop_metrics(run)
            metrics_started = False

            current_phase = RunPhase.ARTIFACT_COLLECTION
            run = self._transition(run, phase=current_phase)
            collected_metadata = self._collect_metadata()

            current_phase = RunPhase.SUMMARIZE
            run = self._transition(run, phase=current_phase)
            self._save_summary(run, workload_result, sanity, collected_metadata)

            return self._transition(run, RunStatus.SUCCESS, RunPhase.DONE)
        except Exception as exc:
            if metrics_started:
                self._stop_metrics_after_failure(run)
            self.repository.save_error(
                ErrorRecord(
                    run_id=self._run_id(run),
                    phase=current_phase,
                    message=str(exc),
                    exception_type=type(exc).__name__,
                )
            )
            return self._transition(run, RunStatus.FAILED, current_phase)

    def _configure_audit(self, spec: RunSpec) -> None:
        if spec.audit_profile.mode is AuditMode.AUDIT_ON:
            self.database_adapter.enable_audit(spec.audit_profile)
            return
        self.database_adapter.disable_audit(spec.audit_profile)

    def _run_precheck(self) -> dict[str, object]:
        self.database_adapter.validate_connectivity()
        return self.database_adapter.run_sanity_checks()

    def _start_metrics(self, run: RunRecord) -> None:
        self.target_host_adapter.start_metrics_collection(self._run_id(run), run.output_dir)

    def _capture_pre_snapshot(self, run: RunRecord, spec: RunSpec) -> None:
        self._register_artifacts(
            self.database_adapter.capture_pre_snapshot(
                self._snapshot_request(run, spec, "pre")
            )
        )

    def _capture_post_snapshot(self, run: RunRecord, spec: RunSpec) -> None:
        self._register_artifacts(
            self.database_adapter.capture_post_snapshot(
                self._snapshot_request(run, spec, "post")
            )
        )

    def _snapshot_request(self, run: RunRecord, spec: RunSpec, label: str) -> SnapshotRequest:
        return SnapshotRequest(
            run_id=self._run_id(run),
            host=spec.target_host,
            output_dir=run.output_dir,
            label=label,
        )

    def _execute_workload(
        self,
        run: RunRecord,
        spec: RunSpec,
    ) -> WorkloadExecutionResult:
        workload_request = WorkloadExecutionRequest(
            run_id=self._run_id(run),
            workload_profile=spec.workload_profile,
            target_host=spec.target_host,
            client_host=spec.client_host,
            audit_profile=spec.audit_profile,
            output_dir=run.output_dir,
        )
        self.workload_runner.prepare_run(workload_request)
        workload_result = self.workload_runner.execute_run(workload_request)
        for path in workload_result.artifacts:
            self.repository.register_artifact(
                RunArtifact(
                    run_id=self._run_id(run),
                    artifact_type="workload_output",
                    path=path,
                    description="Workload runner output",
                )
            )
        if not workload_result.success:
            raise RuntimeError(workload_result.error_message)
        return workload_result

    def _stop_metrics(self, run: RunRecord) -> None:
        self._register_artifacts(
            self.target_host_adapter.stop_metrics_collection(self._run_id(run), run.output_dir)
        )

    def _collect_metadata(self) -> dict[str, object]:
        return {
            "filesystem": self.target_host_adapter.collect_filesystem_stats(),
            "host": self.target_host_adapter.collect_host_metadata(),
            "database": self.database_adapter.collect_database_metadata(),
        }

    def _save_summary(
        self,
        run: RunRecord,
        workload_result: WorkloadExecutionResult,
        sanity: dict[str, object],
        collected_metadata: dict[str, object],
    ) -> None:
        self.repository.save_summary(
            RunSummary(
                run_id=self._run_id(run),
                metrics={
                    "workload": workload_result.metrics,
                    "sanity": sanity,
                    **collected_metadata,
                },
            )
        )

    def _transition(
        self,
        run: RunRecord,
        status: RunStatus | None = None,
        phase: RunPhase | None = None,
    ) -> RunRecord:
        if run.run_id is None:
            raise ValueError("run_id is required for state transitions")
        return self.repository.update_run_status_phase(run.run_id, status=status, phase=phase)

    def _run_id(self, run: RunRecord) -> int:
        return self._required_id(run.run_id, "run_id")

    def _required_id(self, value: int | None, field_name: str) -> int:
        if value is None or value <= 0:
            raise RuntimeError(f"repository did not return a valid {field_name}")
        return value

    def _register_artifacts(self, artifacts: list[RunArtifact]) -> None:
        for artifact in artifacts:
            self.repository.register_artifact(artifact)

    def _stop_metrics_after_failure(self, run: RunRecord) -> None:
        try:
            self._register_artifacts(
                self.target_host_adapter.stop_metrics_collection(self._run_id(run), run.output_dir)
            )
        except Exception as exc:
            self.repository.save_error(
                ErrorRecord(
                    run_id=self._run_id(run),
                    phase=RunPhase.METRICS_STOP,
                    message=str(exc),
                    exception_type=type(exc).__name__,
                )
            )
