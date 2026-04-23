from __future__ import annotations

import argparse
import json
from pathlib import Path

from reporting.dto import ReportGenerationRequest
from reporting.service import BenchmarkReportService


def generate_report_from_db(
    db_path: Path | str,
    markdown_path: Path | str,
    csv_dir: Path | str | None = None,
    include_artifact_fallback: bool = True,
) -> dict[str, object]:
    result = BenchmarkReportService().generate(
        ReportGenerationRequest(
            db_path=Path(db_path),
            markdown_path=Path(markdown_path),
            csv_dir=Path(csv_dir) if csv_dir is not None else None,
            include_artifact_fallback=include_artifact_fallback,
        )
    )
    return result.as_dict()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate a Benchpress Markdown report and CSV exports from SQLite results."
    )
    parser.add_argument("--db", required=True, help="Path to benchpress.sqlite3.")
    parser.add_argument("--out", required=True, help="Markdown report output path.")
    parser.add_argument(
        "--csv-dir",
        help="Directory for CSV exports. Defaults to <markdown parent>/csv.",
    )
    parser.set_defaults(include_artifact_fallback=True)
    parser.add_argument(
        "--include-artifact-fallback",
        dest="include_artifact_fallback",
        action="store_true",
        help="Parse workload artifact key=value output when summaries are missing key metrics.",
    )
    parser.add_argument(
        "--no-artifact-fallback",
        dest="include_artifact_fallback",
        action="store_false",
        help="Only use metrics persisted in run_summaries.metrics_json.",
    )
    args = parser.parse_args(argv)
    report = generate_report_from_db(
        db_path=args.db,
        markdown_path=args.out,
        csv_dir=args.csv_dir,
        include_artifact_fallback=args.include_artifact_fallback,
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
