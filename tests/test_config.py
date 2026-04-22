from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from config.models import BenchmarkConfig, RunTimingConfig
from config.service import BenchmarkConfigService
from orchestration.models import AuditProfile, BenchmarkProfile, HostDefinition, HostRole


class ConfigTests(unittest.TestCase):
    def setUp(self) -> None:
        self.benchmark = BenchmarkProfile("sqlserver-audit")
        self.target = HostDefinition("sql", HostRole.TARGET, "windows", "sql", 4, 16)
        self.client = HostDefinition("client", HostRole.CLIENT, "windows", "client", 2, 4)

    def test_config_matrix_generation(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            config = BenchmarkConfig(
                benchmark_profile=self.benchmark,
                target_host=self.target,
                client_host=self.client,
                audit_profiles=(
                    AuditProfile("off", "audit_off"),
                    AuditProfile("on", "audit_on"),
                ),
                virtual_user_ladder=(10, 20),
                repetitions=2,
                output_root=Path(temp_dir),
            )

            specs = BenchmarkConfigService().build_run_matrix(config)

            self.assertEqual(len(specs), 8)
            self.assertEqual(specs[0].workload_profile.virtual_users, 10)
            self.assertIn("audit_off", str(specs[0].output_root))

    def test_invalid_config_fails_early(self) -> None:
        with self.assertRaises(ValueError):
            RunTimingConfig(measured_minutes=0)
        with self.assertRaises(ValueError):
            BenchmarkConfig(
                benchmark_profile=self.benchmark,
                target_host=self.target,
                client_host=self.client,
                audit_profiles=(),
            )
        with self.assertRaises(ValueError):
            BenchmarkConfig(
                benchmark_profile=self.benchmark,
                target_host=self.target,
                client_host=self.client,
                audit_profiles=(AuditProfile("off", "audit_off"),),
                virtual_user_ladder=(0,),
            )


if __name__ == "__main__":
    unittest.main()

