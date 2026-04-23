from __future__ import annotations

import json
import re
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from observer.constants import (
    ACTIVE_STATUSES,
    DASHBOARD_ACTIVE_LIMIT,
    DASHBOARD_FAILURE_LIMIT,
    DASHBOARD_UPDATED_LIMIT,
    TEXT_PREVIEW_SUFFIXES,
)
from observer.dto import ObserverSessionConfig
from observer.models import ArtifactPreview, ObserverRunState, ObserverSnapshot
from reporting.constants import ARTIFACT_FALLBACK_FILENAMES, THROUGHPUT_METRIC_KEYS, WORKLOAD_SUMMARY_KEY
from reporting.models import ReportSourceRun
from reporting.host_metrics import load_host_metrics
from reporting.repository import ReportingRepository


@dataclass(frozen=True)
class _CachedRunState:
    quick_signature: tuple[Any, ...]
    deep_signature: tuple[Any, ...]
    run_state: ObserverRunState


class ObserverService:
    def __init__(self) -> None:
        self._run_cache: dict[int, _CachedRunState] = {}

    def load_snapshot(self, config: ObserverSessionConfig) -> ObserverSnapshot:
        with ReportingRepository(config.db_path) as repository:
            source_runs = tuple(repository.list_runs())

        active_run_ids = {source_run.run_id for source_run in source_runs}
        self._run_cache = {
            run_id: cached_entry
            for run_id, cached_entry in self._run_cache.items()
            if run_id in active_run_ids
        }
        runs = tuple(self._run_state_for(source_run, config) for source_run in source_runs)
        sorted_by_update = tuple(sorted(runs, key=_updated_sort_key, reverse=True))
        active_runs = tuple(
            run
            for run in sorted_by_update
            if run.status in ACTIVE_STATUSES
        )[:DASHBOARD_ACTIVE_LIMIT]
        recent_failures = tuple(
            run
            for run in sorted_by_update
            if run.has_failures
        )[:DASHBOARD_FAILURE_LIMIT]

        return ObserverSnapshot(
            db_path=config.db_path,
            artifact_root=config.artifact_root,
            collected_at=datetime.now(timezone.utc).isoformat(),
            runs=runs,
            status_counts=dict(Counter(run.status for run in runs)),
            phase_counts=dict(Counter(run.phase for run in runs)),
            active_runs=active_runs,
            recent_failures=recent_failures,
            latest_updated_runs=sorted_by_update[:DASHBOARD_UPDATED_LIMIT],
        )

    def preview_artifact(
        self,
        run: ObserverRunState,
        artifact_index: int,
        config: ObserverSessionConfig,
    ) -> ArtifactPreview:
        if artifact_index < 0 or artifact_index >= len(run.artifacts):
            raise IndexError("artifact_index out of range")

        artifact = run.artifacts[artifact_index]
        path = _resolve_artifact_path(
            artifact.path,
            run.output_dir,
            config.artifact_root,
        )
        if path is None:
            return ArtifactPreview(
                artifact=artifact,
                resolved_path=None,
                previewable=False,
                reason="artifact path is outside the trusted artifact root",
            )
        if not path.exists():
            return ArtifactPreview(
                artifact=artifact,
                resolved_path=path,
                previewable=False,
                reason="artifact file is missing",
            )
        if not path.is_file():
            return ArtifactPreview(
                artifact=artifact,
                resolved_path=path,
                previewable=False,
                reason="artifact path is not a regular file",
            )
        if path.suffix.lower() not in TEXT_PREVIEW_SUFFIXES:
            return ArtifactPreview(
                artifact=artifact,
                resolved_path=path,
                previewable=False,
                reason="only small text artifacts can be previewed inline",
            )
        if path.stat().st_size > config.preview_bytes:
            return ArtifactPreview(
                artifact=artifact,
                resolved_path=path,
                previewable=False,
                reason=f"artifact exceeds preview limit ({config.preview_bytes} bytes)",
            )

        text = path.read_text(encoding="utf-8", errors="replace")
        if path.suffix.lower() == ".json":
            text = _pretty_json_text(text)
        return ArtifactPreview(
            artifact=artifact,
            resolved_path=path,
            previewable=True,
            text=text,
        )

    def _run_state_for(
        self,
        source_run: ReportSourceRun,
        config: ObserverSessionConfig,
    ) -> ObserverRunState:
        quick_signature = _quick_run_signature(source_run, config)
        cached_entry = self._run_cache.get(source_run.run_id)
        if (
            cached_entry is not None
            and cached_entry.quick_signature == quick_signature
            and source_run.status not in ACTIVE_STATUSES
        ):
            return cached_entry.run_state
        deep_signature = _deep_run_signature(source_run, config)
        if (
            cached_entry is not None
            and cached_entry.quick_signature == quick_signature
            and cached_entry.deep_signature == deep_signature
        ):
            return cached_entry.run_state
        run_state = self._build_run_state(source_run, config)
        self._run_cache[source_run.run_id] = _CachedRunState(
            quick_signature=quick_signature,
            deep_signature=deep_signature,
            run_state=run_state,
        )
        return run_state

    def _build_run_state(
        self,
        source_run: ReportSourceRun,
        config: ObserverSessionConfig,
    ) -> ObserverRunState:
        workload_metrics = _summary_workload_metrics(source_run.summary_metrics)
        if config.include_artifact_fallback and _should_read_artifact_fallback(workload_metrics):
            for key, value in _artifact_metrics(source_run, config.artifact_root).items():
                workload_metrics.setdefault(key, value)
        host_metrics, host_samples = load_host_metrics(source_run, config.artifact_root, cache=None)
        return ObserverRunState(
            source_run=source_run,
            workload_metrics=workload_metrics,
            host_metrics=host_metrics,
            host_samples=host_samples,
        )


def _quick_run_signature(source_run: ReportSourceRun, config: ObserverSessionConfig) -> tuple[Any, ...]:
    artifact_ids = tuple(artifact.artifact_id for artifact in source_run.artifacts)
    error_ids = tuple(error.error_id for error in source_run.errors)
    return (
        source_run.run_id,
        source_run.status,
        source_run.phase,
        source_run.updated_at,
        source_run.summary_notes,
        tuple(sorted(source_run.summary_metrics)),
        len(source_run.summary_metrics),
        artifact_ids,
        error_ids,
        source_run.target_memory_gb,
        config.include_artifact_fallback,
    )


def _deep_run_signature(source_run: ReportSourceRun, config: ObserverSessionConfig) -> tuple[Any, ...]:
    return (
        source_run.run_id,
        source_run.status,
        source_run.phase,
        source_run.updated_at,
        source_run.summary_notes,
        json.dumps(source_run.summary_metrics, sort_keys=True, default=str),
        source_run.target_memory_gb,
        config.include_artifact_fallback,
        tuple(
            (
                error.error_id,
                error.phase,
                error.exception_type,
                error.message,
                error.created_at,
            )
            for error in source_run.errors
        ),
        tuple(
            (
                artifact.artifact_id,
                artifact.artifact_type,
                str(artifact.path),
                artifact.description,
                artifact.created_at,
                _path_signature(
                    _resolve_artifact_path(
                        artifact.path,
                        source_run.output_dir,
                        config.artifact_root,
                    )
                ),
            )
            for artifact in source_run.artifacts
        ),
        tuple(
            (filename, _path_signature(_trusted_path(source_run.output_dir / filename, config.artifact_root)))
            for filename in ARTIFACT_FALLBACK_FILENAMES
        ),
    )


def _summary_workload_metrics(summary_metrics: dict[str, Any]) -> dict[str, Any]:
    workload = summary_metrics.get(WORKLOAD_SUMMARY_KEY)
    if isinstance(workload, dict):
        return _normalized_scalar_metrics(workload)
    return _normalized_scalar_metrics(summary_metrics)


def _should_read_artifact_fallback(metrics: dict[str, Any]) -> bool:
    return any(metric_key not in metrics for metric_key in THROUGHPUT_METRIC_KEYS)


def _artifact_metrics(source_run, artifact_root: Path) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    for path in _artifact_fallback_paths(source_run, artifact_root):
        if not path.exists() or not path.is_file():
            continue
        for key, value in _parse_key_value_metrics(path).items():
            metrics.setdefault(key, value)
    return metrics


def _artifact_fallback_paths(source_run, artifact_root: Path) -> Iterable[Path]:
    seen: set[Path] = set()
    for artifact in source_run.artifacts:
        artifact_path = _resolve_artifact_path(artifact.path, source_run.output_dir, artifact_root)
        if artifact_path is None:
            continue
        lower_name = artifact_path.name.lower()
        artifact_type = artifact.artifact_type.lower()
        if (
            lower_name in ARTIFACT_FALLBACK_FILENAMES
            or "workload" in artifact_type
            or "hammerdb" in artifact_type
            or "stdout" in lower_name
        ):
            if artifact_path not in seen:
                seen.add(artifact_path)
                yield artifact_path
    for filename in ARTIFACT_FALLBACK_FILENAMES:
        output_path = _trusted_path(source_run.output_dir / filename, artifact_root)
        if output_path is None:
            continue
        if output_path not in seen:
            seen.add(output_path)
            yield output_path


def _parse_key_value_metrics(path: Path) -> dict[str, Any]:
    metrics: dict[str, Any] = {}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        normalized_key = _normalize_metric_key(key)
        if not normalized_key:
            continue
        metrics[normalized_key] = _coerce_metric_value(value.strip())
    return metrics


def _normalized_scalar_metrics(metrics: dict[str, Any]) -> dict[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in metrics.items():
        if isinstance(value, (dict, list, tuple)):
            continue
        normalized_key = _normalize_metric_key(str(key))
        if not normalized_key:
            continue
        normalized[normalized_key] = _coerce_metric_value(value)
    return normalized


def _normalize_metric_key(key: str) -> str:
    normalized = re.sub(r"[^a-z0-9_]+", "_", key.strip().lower())
    return normalized.strip("_")


def _coerce_metric_value(value: Any) -> Any:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return ""
    numeric_text = text.replace(",", "")
    try:
        return int(numeric_text)
    except ValueError:
        try:
            return float(numeric_text)
        except ValueError:
            return text


def _resolve_artifact_path(path: Path, output_dir: Path, artifact_root: Path) -> Path | None:
    candidate = path if path.is_absolute() or path.exists() else output_dir / path
    return _trusted_path(candidate, artifact_root)


def _trusted_path(path: Path, artifact_root: Path) -> Path | None:
    try:
        resolved_path = path.resolve(strict=False)
        resolved_root = artifact_root.resolve(strict=False)
    except OSError:
        return None
    if resolved_path == resolved_root or resolved_root in resolved_path.parents:
        return path
    return None


def _pretty_json_text(text: str) -> str:
    try:
        loaded = json.loads(text)
    except json.JSONDecodeError:
        return text
    return json.dumps(loaded, indent=2, sort_keys=True)


def _path_signature(path: Path | None) -> tuple[str, int, int] | None:
    if path is None:
        return None
    try:
        stat = path.stat()
    except OSError:
        return None
    return (str(path), stat.st_size, stat.st_mtime_ns)


def _updated_sort_key(run: ObserverRunState) -> tuple[float, int]:
    return (_timestamp_sort_value(run.updated_at), run.run_id)


def _timestamp_sort_value(value: str) -> float:
    try:
        return datetime.fromisoformat(value).timestamp()
    except ValueError:
        return 0.0
