from __future__ import annotations

from pathlib import Path
from typing import Any

from adapters.transport.dto import RemoteCommandRequest
from adapters.transport.service import TransportAdapter
from adapters.workload.constants import WORKLOAD_TOOL_HAMMERDB
from adapters.workload.dto import WorkloadExecutionRequest, WorkloadExecutionResult
from adapters.workload.service import WorkloadRunner


class HammerDBWorkloadRunner(WorkloadRunner):
    COMPLETION_KEY = "benchmark_status"
    COMPLETION_VALUE = "completed"

    def __init__(
        self,
        executable_path: Path | str,
        transport: TransportAdapter | None = None,
        script_path: Path | str | None = None,
        result_filename: str = "hammerdb_stdout.txt",
    ) -> None:
        executable = Path(executable_path)
        if not str(executable).strip():
            raise ValueError("executable_path is required")
        if not result_filename.strip():
            raise ValueError("result_filename is required")
        self.executable_path = executable
        self.transport = transport
        self.script_path = Path(script_path) if script_path is not None else None
        self.result_filename = result_filename

    def prepare_run(self, request: WorkloadExecutionRequest) -> None:
        if request.workload_profile.tool.lower() != WORKLOAD_TOOL_HAMMERDB:
            raise ValueError("HammerDBWorkloadRunner requires a hammerdb workload profile")
        request.output_dir.mkdir(parents=True, exist_ok=True)

    def execute_run(self, request: WorkloadExecutionRequest) -> WorkloadExecutionResult:
        if self.transport is None:
            raise NotImplementedError("HammerDB execution requires a transport adapter")
        if self.script_path is None:
            raise NotImplementedError("script_path is not configured")
        command = f'"{self.executable_path}" auto "{self.script_path}"'
        result = self.transport.execute_command(
            RemoteCommandRequest(
                host=request.client_host,
                command=command,
                timeout_seconds=self._timeout_seconds(request),
            )
        )
        raw_output_path = request.output_dir / self.result_filename
        raw_output_path.write_text(result.stdout, encoding="utf-8")
        artifacts: list[Path] = [raw_output_path]
        stderr_output_path = self._stderr_output_path(raw_output_path, result.stderr)
        if stderr_output_path is not None:
            artifacts.append(stderr_output_path)
        if not result.succeeded:
            return WorkloadExecutionResult(
                success=False,
                artifacts=tuple(artifacts),
                raw_output_path=raw_output_path,
                error_message=result.stderr or result.stdout or "HammerDB execution failed",
            )
        metrics = self.parse_results(raw_output_path)
        validation_error = self._validation_error(result.stdout, result.stderr, metrics)
        if validation_error is not None:
            return WorkloadExecutionResult(
                success=False,
                artifacts=tuple(artifacts),
                raw_output_path=raw_output_path,
                error_message=validation_error,
            )
        return WorkloadExecutionResult(
            success=True,
            artifacts=tuple(artifacts),
            metrics=metrics,
            raw_output_path=raw_output_path,
        )

    def parse_results(self, result_path: Path) -> dict[str, Any]:
        result_path = Path(result_path)
        if not result_path.exists():
            raise FileNotFoundError(result_path)
        metrics: dict[str, Any] = {}
        for line in result_path.read_text(encoding="utf-8").splitlines():
            if "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()
            if not key:
                continue
            metrics[key] = self._coerce_metric_value(value)
        return metrics

    def _timeout_seconds(self, request: WorkloadExecutionRequest) -> int:
        minutes = (
            request.workload_profile.warmup_minutes
            + request.workload_profile.measured_minutes
            + request.workload_profile.cooldown_minutes
        )
        return max(60, minutes * 60)

    def _stderr_output_path(self, raw_output_path: Path, stderr: str) -> Path | None:
        if not stderr.strip():
            return None
        stderr_output_path = raw_output_path.with_name(
            raw_output_path.stem + "_stderr" + raw_output_path.suffix
        )
        stderr_output_path.write_text(stderr, encoding="utf-8")
        return stderr_output_path

    def _validation_error(
        self,
        stdout: str,
        stderr: str,
        metrics: dict[str, Any],
    ) -> str | None:
        combined_output = "\n".join(part for part in (stdout, stderr) if part).lower()
        if "usage: hammerdb" in combined_output:
            return "HammerDB returned usage output; check the executable path and auto-mode invocation."
        benchmark_status = str(metrics.get(self.COMPLETION_KEY, "")).strip().lower()
        if benchmark_status == self.COMPLETION_VALUE:
            return None
        detail = self._result_excerpt(stderr or stdout)
        message = (
            "HammerDB run did not report "
            f"{self.COMPLETION_KEY}={self.COMPLETION_VALUE}."
        )
        if detail:
            return f"{message} Output: {detail}"
        return message

    def _result_excerpt(self, value: str) -> str:
        compact = " ".join(value.split())
        return compact[:200]

    def _coerce_metric_value(self, value: str) -> Any:
        try:
            return int(value)
        except ValueError:
            try:
                return float(value)
            except ValueError:
                return value
