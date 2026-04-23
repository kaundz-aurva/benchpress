# Repository Guidelines

## Project Structure & Module Organization

This repository is a Python benchmark orchestration framework. Keep responsibilities separated by package:

- `orchestration/` for benchmark domain models, DTOs, and lifecycle coordination.
- `config/` for typed benchmark configuration and run-matrix generation.
- `db/` for SQLite migrations and repository methods.
- `adapters/` for database, host, transport, and workload interfaces plus concrete implementations.
- `agents/` for FastAPI VM agents and agent HTTP clients.
- `scripts/` for generated SQL/TCL/PowerShell benchmark assets.
- `examples/` for JSON runtime specs.
- `tests/` for isolated standard-library `unittest` coverage.

Prefer small, cohesive modules over broad utility files. Keep test files close to the behavior they verify, either under `tests/` with matching paths or beside source files if the chosen language ecosystem expects that pattern.

## Build, Test, and Development Commands

Use the project virtual environment and pinned dependencies:

- `python3 -m venv env && source env/bin/activate && python -m pip install -r requirements.txt`: create/install a local environment.
- `env/bin/python -m unittest discover -s tests`: run the full test suite.
- `env/bin/python -m compileall agents adapters config orchestration db scripts tests benchpress_orchestrator.py sqlserver_agent.py generate_benchmark_assets.py`: check syntax.

Document required environment variables in `.env.example`, never in committed `.env` files.

## Coding Style & Naming Conventions

Use type hints throughout Python code and prefer dataclasses/enums for domain shapes. Use Pydantic DTOs at FastAPI and JSON-spec boundaries. Keep SQLite SQL in `db/`, generated benchmark asset logic in `scripts/`, agent-side SQL Server commands in `agents/sqlserver/`, and adapter behavior under `adapters/`. Use 4-space Python indentation, `snake_case` for functions, methods, variables, and test names, and `PascalCase` for classes and enums.

## Testing Guidelines

Every behavioral change should include focused tests. Use temp directories and temp SQLite databases; tests must not require real SQL Server, Windows VMs, HammerDB, or network access. Prefer fake adapters over brittle mocks for orchestration tests.

## Commit & Pull Request Guidelines

This repository has no established commit history yet. Use concise, imperative commit messages such as `Add benchmark runner` or `Persist run summaries`. Pull requests should include the problem being solved, main implementation choices, test evidence, and follow-up gaps. Link related issues when available.
