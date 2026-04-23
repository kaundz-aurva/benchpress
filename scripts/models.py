from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class GeneratedBenchmarkAssets:
    output_dir: Path
    audit_enable_sql: Path
    audit_disable_sql: Path
    audit_pre_snapshot_sql: Path
    audit_post_snapshot_sql: Path
    audit_metadata_sql: Path
    hammerdb_tcl: Path
    metrics_start_ps1: Path
    metrics_stop_ps1: Path
    agent_config_json: Path

    def as_dict(self) -> dict[str, str]:
        return {
            "output_dir": str(self.output_dir),
            "audit_enable_sql": str(self.audit_enable_sql),
            "audit_disable_sql": str(self.audit_disable_sql),
            "audit_pre_snapshot_sql": str(self.audit_pre_snapshot_sql),
            "audit_post_snapshot_sql": str(self.audit_post_snapshot_sql),
            "audit_metadata_sql": str(self.audit_metadata_sql),
            "hammerdb_tcl": str(self.hammerdb_tcl),
            "metrics_start_ps1": str(self.metrics_start_ps1),
            "metrics_stop_ps1": str(self.metrics_stop_ps1),
            "agent_config_json": str(self.agent_config_json),
        }

