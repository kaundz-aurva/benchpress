from __future__ import annotations

import argparse
from pathlib import Path
from typing import Callable

from config.service import load_runtime_spec
from observer.constants import DEFAULT_REFRESH_SECONDS
from observer.dto import ObserverSessionConfig


ObserverLauncher = Callable[[ObserverSessionConfig], None]


def main(
    argv: list[str] | None = None,
    launch_fn: ObserverLauncher | None = None,
) -> int:
    parser = argparse.ArgumentParser(
        description="Observe Benchpress runs from SQLite in a live terminal UI."
    )
    source_group = parser.add_mutually_exclusive_group(required=True)
    source_group.add_argument("--db", help="Path to benchpress.sqlite3.")
    source_group.add_argument("--spec", help="Path to a benchmark runtime JSON spec.")
    parser.add_argument(
        "--artifact-root",
        help="Trusted base directory for run artifacts. Defaults to the DB parent, or storage.output_root when --spec is used.",
    )
    parser.add_argument(
        "--refresh-seconds",
        type=float,
        default=DEFAULT_REFRESH_SECONDS,
        help="Default auto-refresh interval in seconds. Default: 2.",
    )
    args = parser.parse_args(argv)
    session_config = _session_config_from_args(args)
    launcher = launch_fn or _default_launch_observer
    launcher(session_config)
    return 0


def _session_config_from_args(args: argparse.Namespace) -> ObserverSessionConfig:
    if args.spec:
        spec = load_runtime_spec(args.spec)
        db_path = spec.storage.sqlite_path
        artifact_root = Path(args.artifact_root) if args.artifact_root else spec.storage.output_root
    else:
        db_path = Path(args.db)
        artifact_root = Path(args.artifact_root) if args.artifact_root else db_path.parent
    return ObserverSessionConfig(
        db_path=db_path,
        artifact_root=artifact_root,
        refresh_seconds=args.refresh_seconds,
    )


def _default_launch_observer(session_config: ObserverSessionConfig) -> None:
    try:
        from observer.ui import launch_observer_app
    except ImportError as exc:
        raise RuntimeError(
            "textual is required to run the Benchpress observer. Install the project requirements first."
        ) from exc
    launch_observer_app(session_config)


if __name__ == "__main__":
    raise SystemExit(main())
