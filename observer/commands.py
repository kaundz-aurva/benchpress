from __future__ import annotations

from dataclasses import dataclass


class CommandParseError(ValueError):
    pass


@dataclass(frozen=True)
class ObserverCommand:
    name: str
    value: int | float | None = None


def parse_command(text: str) -> ObserverCommand:
    command_text = text.strip()
    if command_text.startswith(":"):
        command_text = command_text[1:].lstrip()
    if not command_text:
        raise CommandParseError("command is required")

    name, _, argument = command_text.partition(" ")
    normalized_name = name.strip().lower()
    normalized_argument = argument.strip()

    if normalized_name in {"q", "quit"}:
        return ObserverCommand("quit")
    if normalized_name == "runs":
        return ObserverCommand("runs")
    if normalized_name in {"failures", "failure"}:
        return ObserverCommand("failures")
    if normalized_name in {"dashboard", "dash"}:
        return ObserverCommand("dashboard")
    if normalized_name == "help":
        return ObserverCommand("help")
    if normalized_name in {"reload", "r"}:
        return ObserverCommand("reload")
    if normalized_name == "open":
        if not normalized_argument:
            raise CommandParseError("open requires a run id")
        try:
            run_id = int(normalized_argument)
        except ValueError as exc:
            raise CommandParseError("open requires an integer run id") from exc
        if run_id <= 0:
            raise CommandParseError("open requires a positive run id")
        return ObserverCommand("open", run_id)
    if normalized_name == "refresh":
        if not normalized_argument:
            raise CommandParseError("refresh requires a positive number of seconds")
        try:
            refresh_seconds = float(normalized_argument)
        except ValueError as exc:
            raise CommandParseError("refresh requires a numeric value in seconds") from exc
        if refresh_seconds <= 0:
            raise CommandParseError("refresh requires a positive number of seconds")
        return ObserverCommand("refresh", refresh_seconds)

    raise CommandParseError(f"unknown command: {normalized_name}")
