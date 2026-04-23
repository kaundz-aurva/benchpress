from __future__ import annotations

import csv
import json
from collections.abc import Iterable
from dataclasses import asdict
from pathlib import Path
from statistics import mean
from typing import Any

from reporting.models import HostMetricSample, ReportSourceRun


class HostMetricsCache:
    def __init__(self, cache_path: Path) -> None:
        self.cache_path = Path(cache_path)
        self._entries = self._load_entries()
        self._dirty = False

    def get_samples(self, path: Path, target_memory_gb: int) -> list[dict[str, Any]] | None:
        signature = _artifact_signature(path, target_memory_gb)
        if signature is None:
            return None
        entry = self._entries.get(_cache_key(path, target_memory_gb))
        if not isinstance(entry, dict) or entry.get("signature") != signature:
            return None
        samples = entry.get("samples")
        if not isinstance(samples, list):
            return None
        return [sample for sample in samples if isinstance(sample, dict)]

    def put_samples(
        self,
        path: Path,
        target_memory_gb: int,
        samples: tuple[HostMetricSample, ...],
    ) -> None:
        signature = _artifact_signature(path, target_memory_gb)
        if signature is None:
            return
        self._entries[_cache_key(path, target_memory_gb)] = {
            "signature": signature,
            "samples": [_sample_payload(sample) for sample in samples],
        }
        self._dirty = True

    def save(self) -> None:
        if not self._dirty:
            return
        self.cache_path.parent.mkdir(parents=True, exist_ok=True)
        self.cache_path.write_text(
            json.dumps(self._entries, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        self._dirty = False

    def _load_entries(self) -> dict[str, Any]:
        if not self.cache_path.exists():
            return {}
        try:
            loaded = json.loads(self.cache_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        if not isinstance(loaded, dict):
            return {}
        return loaded


def load_host_metrics(
    source_run: ReportSourceRun,
    artifact_root: Path,
    cache: HostMetricsCache | None = None,
) -> tuple[dict[str, float | int], tuple[HostMetricSample, ...]]:
    samples: list[HostMetricSample] = []
    next_index = 1
    for path in _host_metric_paths(source_run, artifact_root):
        parsed_samples = _load_cached_or_parse(path, source_run, cache)
        for sample in parsed_samples:
            samples.append(_copy_sample(sample, source_run.run_id, next_index))
            next_index += 1
    return _summarize_samples(samples), tuple(samples)


def _load_cached_or_parse(
    path: Path,
    source_run: ReportSourceRun,
    cache: HostMetricsCache | None,
) -> tuple[HostMetricSample, ...]:
    if cache is not None:
        cached_samples = cache.get_samples(path, source_run.target_memory_gb)
        if cached_samples is not None:
            return tuple(
                _sample_from_payload(payload, source_run.run_id, index + 1)
                for index, payload in enumerate(cached_samples)
            )
    parsed_samples = tuple(
        _parse_perfmon_csv(
            path,
            run_id=source_run.run_id,
            start_index=1,
            target_memory_gb=source_run.target_memory_gb,
        )
    )
    if cache is not None and parsed_samples:
        cache.put_samples(path, source_run.target_memory_gb, parsed_samples)
    return parsed_samples


def _host_metric_paths(source_run: ReportSourceRun, artifact_root: Path) -> Iterable[Path]:
    seen: set[Path] = set()
    for artifact in source_run.artifacts:
        path = _resolve_artifact_path(artifact.path, source_run.output_dir, artifact_root)
        if path is None:
            continue
        artifact_type = artifact.artifact_type.lower()
        name = path.name.lower()
        if artifact_type == "host_metrics_csv" or (
            "host_metrics" in artifact_type and name.endswith(".csv")
        ):
            if path not in seen:
                seen.add(path)
                yield path


def _parse_perfmon_csv(
    path: Path,
    run_id: int,
    start_index: int,
    target_memory_gb: int,
) -> list[HostMetricSample]:
    if not path.exists() or not path.is_file():
        return []
    with path.open("r", encoding="utf-8-sig", errors="replace", newline="") as file_obj:
        reader = csv.DictReader(file_obj)
        fieldnames = list(reader.fieldnames or [])
        if not fieldnames or fieldnames[0] is None:
            return []
        timestamp_header = fieldnames[0]
        counter_map = {
            header: _counter_metric_name(header)
            for header in fieldnames[1:]
        }
        samples: list[HostMetricSample] = []
        for row in reader:
            values = {
                metric_name: _parse_float(row.get(header, ""))
                for header, metric_name in counter_map.items()
                if metric_name is not None
            }
            values = {key: value for key, value in values.items() if value is not None}
            if not values:
                continue
            available_memory_mb = values.get("available_memory_mb")
            memory_used_mb = _memory_used_mb(available_memory_mb, target_memory_gb)
            memory_used_percent = _memory_used_percent(memory_used_mb, target_memory_gb)
            samples.append(
                HostMetricSample(
                    run_id=run_id,
                    sample_index=start_index + len(samples),
                    timestamp=str(row.get(timestamp_header, "") or "").strip(),
                    total_cpu_percent=values.get("total_cpu_percent"),
                    sql_cpu_percent=values.get("sql_cpu_percent"),
                    available_memory_mb=available_memory_mb,
                    memory_used_mb=memory_used_mb,
                    memory_used_percent=memory_used_percent,
                    sql_working_set_mb=_bytes_to_mb(values.get("sql_working_set_bytes")),
                )
            )
    return samples


def _summarize_samples(samples: list[HostMetricSample]) -> dict[str, float | int]:
    if not samples:
        return {}
    summary: dict[str, float | int] = {"sample_count": len(samples)}
    _add_average_and_max(
        summary,
        "total_cpu_percent",
        (sample.total_cpu_percent for sample in samples),
    )
    _add_average_and_max(
        summary,
        "sql_cpu_percent",
        (sample.sql_cpu_percent for sample in samples),
    )
    _add_average_and_min(
        summary,
        "available_memory_mb",
        (sample.available_memory_mb for sample in samples),
    )
    _add_average_and_max(
        summary,
        "memory_used_mb",
        (sample.memory_used_mb for sample in samples),
    )
    _add_average_and_max(
        summary,
        "memory_used_percent",
        (sample.memory_used_percent for sample in samples),
    )
    _add_average_and_max(
        summary,
        "sql_working_set_mb",
        (sample.sql_working_set_mb for sample in samples),
    )
    return summary


def _counter_metric_name(header: str) -> str | None:
    normalized = header.strip().lower()
    if "\\processor(_total)\\% processor time" in normalized:
        return "total_cpu_percent"
    if "\\process(sqlservr)\\% processor time" in normalized:
        return "sql_cpu_percent"
    if "\\memory\\available mbytes" in normalized:
        return "available_memory_mb"
    if "\\process(sqlservr)\\working set" in normalized:
        return "sql_working_set_bytes"
    return None


def _add_average_and_max(
    summary: dict[str, float | int],
    key: str,
    values: Iterable[float | None],
) -> None:
    numeric_values = [value for value in values if value is not None]
    if not numeric_values:
        return
    summary[f"{key}_avg"] = float(mean(numeric_values))
    summary[f"{key}_max"] = max(numeric_values)


def _add_average_and_min(
    summary: dict[str, float | int],
    key: str,
    values: Iterable[float | None],
) -> None:
    numeric_values = [value for value in values if value is not None]
    if not numeric_values:
        return
    summary[f"{key}_avg"] = float(mean(numeric_values))
    summary[f"{key}_min"] = min(numeric_values)


def _memory_used_mb(available_memory_mb: float | None, target_memory_gb: int) -> float | None:
    if available_memory_mb is None or target_memory_gb <= 0:
        return None
    total_memory_mb = target_memory_gb * 1024.0
    return min(max(total_memory_mb - available_memory_mb, 0.0), total_memory_mb)


def _memory_used_percent(memory_used_mb: float | None, target_memory_gb: int) -> float | None:
    if memory_used_mb is None or target_memory_gb <= 0:
        return None
    return (memory_used_mb / (target_memory_gb * 1024.0)) * 100.0


def _bytes_to_mb(value: float | None) -> float | None:
    if value is None:
        return None
    return value / (1024.0 * 1024.0)


def _parse_float(value: str | None) -> float | None:
    if value is None:
        return None
    text = value.strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


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


def _artifact_signature(path: Path, target_memory_gb: int) -> dict[str, Any] | None:
    try:
        stat = path.stat()
    except OSError:
        return None
    return {
        "path": str(path),
        "size": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
        "target_memory_gb": target_memory_gb,
    }


def _cache_key(path: Path, target_memory_gb: int) -> str:
    return f"{path}|{target_memory_gb}"


def _sample_payload(sample: HostMetricSample) -> dict[str, Any]:
    payload = asdict(sample)
    payload.pop("run_id", None)
    payload.pop("sample_index", None)
    return payload


def _sample_from_payload(
    payload: dict[str, Any],
    run_id: int,
    sample_index: int,
) -> HostMetricSample:
    return HostMetricSample(
        run_id=run_id,
        sample_index=sample_index,
        timestamp=str(payload.get("timestamp") or ""),
        total_cpu_percent=_optional_float(payload.get("total_cpu_percent")),
        sql_cpu_percent=_optional_float(payload.get("sql_cpu_percent")),
        available_memory_mb=_optional_float(payload.get("available_memory_mb")),
        memory_used_mb=_optional_float(payload.get("memory_used_mb")),
        memory_used_percent=_optional_float(payload.get("memory_used_percent")),
        sql_working_set_mb=_optional_float(payload.get("sql_working_set_mb")),
    )


def _copy_sample(sample: HostMetricSample, run_id: int, sample_index: int) -> HostMetricSample:
    return HostMetricSample(
        run_id=run_id,
        sample_index=sample_index,
        timestamp=sample.timestamp,
        total_cpu_percent=sample.total_cpu_percent,
        sql_cpu_percent=sample.sql_cpu_percent,
        available_memory_mb=sample.available_memory_mb,
        memory_used_mb=sample.memory_used_mb,
        memory_used_percent=sample.memory_used_percent,
        sql_working_set_mb=sample.sql_working_set_mb,
    )


def _optional_float(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int) or isinstance(value, float):
        return float(value)
    try:
        return float(str(value))
    except ValueError:
        return None
