from __future__ import annotations

import argparse
from pathlib import Path

import uvicorn

from agents.sqlserver.app import create_app
from agents.sqlserver.models import SqlServerAgentConfig
from agents.sqlserver.service import SqlServerAgentService


def build_app_from_config(config_path: Path | str):
    config = SqlServerAgentConfig.from_json_file(config_path)
    return create_app(
        service=SqlServerAgentService(config),
        bearer_token=config.resolve_bearer_token(),
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the Benchpress SQL Server VM agent.")
    parser.add_argument("--config", required=True, help="Path to SQL Server agent JSON config.")
    parser.add_argument("--host", default="0.0.0.0", help="FastAPI bind host.")
    parser.add_argument("--port", type=int, default=8080, help="FastAPI bind port.")
    args = parser.parse_args(argv)
    app = build_app_from_config(args.config)
    uvicorn.run(app, host=args.host, port=args.port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

