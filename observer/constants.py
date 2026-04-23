from __future__ import annotations

DEFAULT_REFRESH_SECONDS = 2.0
DEFAULT_PREVIEW_BYTES = 64 * 1024

SUCCESS_STATUS = "success"
ACTIVE_STATUSES = ("pending", "running")

TEXT_PREVIEW_SUFFIXES = (
    ".txt",
    ".log",
    ".json",
    ".sql",
    ".csv",
    ".md",
    ".ps1",
    ".tcl",
    ".yaml",
    ".yml",
)

DASHBOARD_ACTIVE_LIMIT = 8
DASHBOARD_FAILURE_LIMIT = 8
DASHBOARD_UPDATED_LIMIT = 10
