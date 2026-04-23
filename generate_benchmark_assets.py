from __future__ import annotations

import argparse
import json
from pathlib import Path

from config.service import load_runtime_spec
from scripts.service import BenchmarkAssetGenerationService


def generate_assets_from_spec(spec_path: Path | str, output_dir: Path | str) -> dict[str, str]:
    spec = load_runtime_spec(spec_path)
    assets = BenchmarkAssetGenerationService().generate(spec, output_dir)
    return assets.as_dict()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Generate Benchpress SQL/HammerDB/metrics assets.")
    parser.add_argument("--spec", required=True, help="Path to benchmark runtime JSON spec.")
    parser.add_argument("--out", required=True, help="Directory where generated assets will be written.")
    args = parser.parse_args(argv)
    print(json.dumps(generate_assets_from_spec(args.spec, args.out), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

