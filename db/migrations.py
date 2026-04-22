from __future__ import annotations

import sqlite3


SCHEMA_STATEMENTS: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS benchmark_profiles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        database_engine TEXT NOT NULL,
        database_version TEXT NOT NULL,
        cloud_provider TEXT NOT NULL,
        description TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS hosts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        role TEXT NOT NULL,
        os_type TEXT NOT NULL,
        hostname TEXT NOT NULL,
        vcpus INTEGER NOT NULL,
        memory_gb INTEGER NOT NULL,
        cloud_instance_id TEXT NOT NULL DEFAULT '',
        metadata_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS workload_profiles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        tool TEXT NOT NULL,
        virtual_users INTEGER NOT NULL,
        warmup_minutes INTEGER NOT NULL,
        measured_minutes INTEGER NOT NULL,
        cooldown_minutes INTEGER NOT NULL,
        metadata_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS audit_profiles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        mode TEXT NOT NULL,
        config_json TEXT NOT NULL DEFAULT '{}',
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS runs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        benchmark_profile_id INTEGER NOT NULL REFERENCES benchmark_profiles(id),
        target_host_id INTEGER NOT NULL REFERENCES hosts(id),
        client_host_id INTEGER NOT NULL REFERENCES hosts(id),
        workload_profile_id INTEGER NOT NULL REFERENCES workload_profiles(id),
        audit_profile_id INTEGER NOT NULL REFERENCES audit_profiles(id),
        repetition INTEGER NOT NULL,
        status TEXT NOT NULL,
        phase TEXT NOT NULL,
        output_dir TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS run_artifacts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
        artifact_type TEXT NOT NULL,
        path TEXT NOT NULL,
        description TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS run_summaries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER NOT NULL UNIQUE REFERENCES runs(id) ON DELETE CASCADE,
        metrics_json TEXT NOT NULL,
        notes TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS run_errors (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        run_id INTEGER NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
        phase TEXT NOT NULL,
        message TEXT NOT NULL,
        exception_type TEXT NOT NULL DEFAULT '',
        created_at TEXT NOT NULL
    )
    """,
)


def initialize_schema(connection: sqlite3.Connection) -> None:
    connection.execute("PRAGMA foreign_keys = ON")
    for statement in SCHEMA_STATEMENTS:
        connection.execute(statement)
    connection.commit()

