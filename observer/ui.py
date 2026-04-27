from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import datetime

from rich.console import Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from textual import events
from textual.app import App, ComposeResult
from textual.containers import Horizontal, VerticalScroll
from textual.timer import Timer
from textual.widgets import Input, Static

from observer.commands import CommandParseError, ObserverCommand, parse_command
from observer.dto import ObserverSessionConfig
from observer.models import ObserverRunState, ObserverSnapshot, TextPreview
from observer.service import ObserverService


class BenchpressObserverApp(App[None]):
    CSS = """
    Screen {
        layout: vertical;
        background: $surface;
        color: $text;
    }

    #titlebar {
        height: 1;
        padding: 0 1;
        background: $primary-darken-2;
        color: $text;
        content-align: left middle;
    }

    #content-scroll {
        height: 1fr;
        padding: 0 1;
    }

    #content {
        width: 100%;
    }

    #statusbar {
        height: 2;
        padding: 0 1;
        background: $boost;
        color: $text;
    }

    #command-bar {
        height: 3;
        padding: 0 1;
        background: $panel;
    }

    #command-bar.hidden {
        display: none;
    }

    #command-prefix {
        width: 3;
        content-align: center middle;
        color: $warning;
    }

    #command-input {
        width: 1fr;
    }
    """

    def __init__(self, session_config: ObserverSessionConfig) -> None:
        super().__init__()
        self.session_config = session_config
        self.refresh_seconds = float(session_config.refresh_seconds)
        self.service = ObserverService()
        self.snapshot: ObserverSnapshot | None = None
        self.view_mode = "dashboard"
        self.detail_mode = "general"
        self.detail_return_view = "runs"
        self.return_view = "dashboard"
        self.selected_run_id: int | None = None
        self.selected_artifact_index = 0
        self.text_preview: TextPreview | None = None
        self.status_message = "Loading observer state..."
        self.refresh_timer: Timer | None = None

    def compose(self) -> ComposeResult:
        yield Static("", id="titlebar")
        with VerticalScroll(id="content-scroll"):
            yield Static("", id="content")
        yield Static("", id="statusbar")
        with Horizontal(id="command-bar", classes="hidden"):
            yield Static(":", id="command-prefix")
            yield Input(
                placeholder="q | dashboard | runs | failures | open 12 | refresh 5",
                id="command-input",
            )

    def on_mount(self) -> None:
        self._refresh_snapshot("Observer attached")
        self._restart_refresh_timer()

    def on_key(self, event: events.Key) -> None:
        if self._command_is_open():
            if event.key == "escape":
                self._close_command("Command cancelled")
                event.stop()
            return

        character = (event.character or "").lower()
        if character == ":":
            self._open_command()
            event.stop()
            return
        if character == "?":
            self._toggle_help()
            event.stop()
            return
        if event.key == "r":
            self._refresh_snapshot("Manual refresh")
            event.stop()
            return
        if self.view_mode == "detail" and character == "e":
            if self._open_latest_error_preview():
                event.stop()
                return
        if self.view_mode == "detail" and character == "s":
            if self._open_failure_summary_preview():
                event.stop()
                return
        if event.key == "up":
            if self.view_mode == "text":
                self._content_scroll().scroll_up(animate=False, immediate=True)
                event.stop()
                return
            if self.view_mode in {"runs", "failures"}:
                self._move_run_selection(-1)
                event.stop()
                return
            if self.view_mode == "detail":
                self._move_artifact_selection(-1)
                event.stop()
                return
        if event.key == "down":
            if self.view_mode == "text":
                self._content_scroll().scroll_down(animate=False, immediate=True)
                event.stop()
                return
            if self.view_mode in {"runs", "failures"}:
                self._move_run_selection(1)
                event.stop()
                return
            if self.view_mode == "detail":
                self._move_artifact_selection(1)
                event.stop()
                return
        if self.view_mode == "text" and event.key == "left":
            self._content_scroll().scroll_left(animate=False, immediate=True)
            event.stop()
            return
        if self.view_mode == "text" and event.key == "right":
            self._content_scroll().scroll_right(animate=False, immediate=True)
            event.stop()
            return
        if self.view_mode == "text" and event.key == "pageup":
            self._content_scroll().scroll_page_up(animate=False)
            event.stop()
            return
        if self.view_mode == "text" and event.key == "pagedown":
            self._content_scroll().scroll_page_down(animate=False)
            event.stop()
            return
        if self.view_mode == "text" and event.key == "home":
            self._content_scroll().scroll_home(animate=False, immediate=True)
            event.stop()
            return
        if self.view_mode == "text" and event.key == "end":
            self._content_scroll().scroll_end(animate=False, immediate=True)
            event.stop()
            return
        if event.key == "enter":
            if self._enter_current_selection():
                event.stop()
                return
        if event.key in {"escape", "backspace"}:
            if self._navigate_back():
                event.stop()
                return
        if self.view_mode != "text" and event.key == "left":
            if self._navigate_back():
                event.stop()
                return

    def on_input_submitted(self, event: Input.Submitted) -> None:
        if event.input.id != "command-input":
            return
        command_text = event.value
        self._close_command()
        self._run_command(command_text)

    def _open_command(self) -> None:
        command_bar = self.query_one("#command-bar", Horizontal)
        command_bar.remove_class("hidden")
        command_input = self.query_one("#command-input", Input)
        command_input.value = ""
        command_input.focus()
        self.status_message = "Command mode"
        self._render()

    def _close_command(self, message: str | None = None) -> None:
        command_bar = self.query_one("#command-bar", Horizontal)
        command_bar.add_class("hidden")
        command_input = self.query_one("#command-input", Input)
        command_input.value = ""
        if message is not None:
            self.status_message = message
        self._render()

    def _command_is_open(self) -> bool:
        return not self.query_one("#command-bar", Horizontal).has_class("hidden")

    def _content_scroll(self) -> VerticalScroll:
        return self.query_one("#content-scroll", VerticalScroll)

    def _restart_refresh_timer(self) -> None:
        if self.refresh_timer is not None:
            self.refresh_timer.stop()
        self.refresh_timer = self.set_interval(self.refresh_seconds, self._refresh_snapshot)

    def _refresh_snapshot(self, message: str | None = None) -> None:
        try:
            self.snapshot = self.service.load_snapshot(self.session_config)
        except Exception as exc:
            self.status_message = f"Refresh failed: {exc}"
        else:
            self._reconcile_selection()
            if message is not None:
                self.status_message = message
        self._render()

    def _reconcile_selection(self) -> None:
        if self.snapshot is None or not self.snapshot.runs:
            self.selected_run_id = None
            self.selected_artifact_index = 0
            return

        selectable_runs = self._run_collection_for_view()
        if self.view_mode == "failures" and not selectable_runs:
            self.selected_run_id = None
            self.selected_artifact_index = 0
            return
        if not selectable_runs:
            selectable_runs = self.snapshot.runs

        if self.selected_run_id is None or not any(
            run.run_id == self.selected_run_id for run in selectable_runs
        ):
            self.selected_run_id = selectable_runs[0].run_id
            self.selected_artifact_index = 0
            return

        selected_run = self.snapshot.find_run(self.selected_run_id)
        if selected_run is None:
            self.selected_artifact_index = 0
            return

        artifacts = self._selected_artifacts(selected_run)
        if artifacts:
            self.selected_artifact_index = min(
                self.selected_artifact_index,
                len(artifacts) - 1,
            )
        else:
            self.selected_artifact_index = 0

    def _run_collection_for_view(self) -> tuple[ObserverRunState, ...]:
        if self.snapshot is None:
            return ()
        if self.view_mode == "failures":
            return self.snapshot.failure_runs
        return self.snapshot.runs

    def _selected_artifacts(self, run: ObserverRunState | None = None) -> tuple:
        selected_run = run or self._selected_run()
        if selected_run is None:
            return ()
        if self.detail_mode == "triage":
            return selected_run.triage_artifacts
        return selected_run.artifacts

    def _render(self) -> None:
        self.query_one("#titlebar", Static).update(self._title_text())
        self.query_one("#content", Static).update(self._render_content())
        self.query_one("#statusbar", Static).update(self._status_text())

    def _title_text(self) -> Text:
        snapshot_suffix = ""
        if self.snapshot is not None:
            snapshot_suffix = f" | {self.snapshot.db_path}"
        if self.view_mode == "detail":
            view_label = f"detail:{self.detail_mode}"
        elif self.view_mode == "text":
            view_label = "text-viewer"
        else:
            view_label = self.view_mode
        return Text(f"Benchpress Observer | {view_label}{snapshot_suffix}", style="bold white")

    def _status_text(self) -> Text:
        refresh_text = f"refresh={self.refresh_seconds:g}s"
        loaded_text = ""
        if self.snapshot is not None:
            loaded_text = f" | loaded {self._display_time(self.snapshot.collected_at)}"
        if self.view_mode == "text":
            action_text = (
                " Esc back  Up/Down scroll  Left/Right pan  PgUp/PgDn page "
                "Home/End jump  terminal selection copy"
            )
        elif self.view_mode == "detail":
            action_text = " : command  ? help  Enter text  e latest error  s summary  Esc back"
        elif self.view_mode == "failures":
            action_text = " : command  ? help  Enter triage detail  Up/Down select  Esc back  r reload"
        elif self.view_mode == "runs":
            action_text = " : command  ? help  Enter detail  Up/Down select  Esc back  r reload"
        else:
            action_text = " : command  ? help  Enter drill down  Esc back  r reload"
        message = f" | {self.status_message}" if self.status_message else ""
        return Text(f"{action_text} | {refresh_text}{loaded_text}{message}")

    def _render_content(self):
        if self.snapshot is None:
            return Panel("Loading Benchpress observer state...", border_style="cyan")
        if self.view_mode == "dashboard":
            return self._render_dashboard()
        if self.view_mode == "runs":
            return self._render_runs_table()
        if self.view_mode == "failures":
            return self._render_failures_table()
        if self.view_mode == "detail":
            return self._render_run_detail()
        if self.view_mode == "text":
            return self._render_text_preview()
        return self._render_help()

    def _render_dashboard(self):
        snapshot = self.snapshot
        if snapshot is None:
            return Panel("No snapshot loaded.", border_style="yellow")
        summary = Group(
            self._summary_panel("Status Counts", snapshot.status_counts),
            self._summary_panel("Phase Counts", snapshot.phase_counts),
        )
        return Group(
            Panel(summary, title="Dashboard", border_style="green"),
            self._run_collection_panel("Active Runs", snapshot.active_runs),
            self._failure_collection_panel("Recent Failures", snapshot.recent_failures),
            self._run_collection_panel("Latest Updates", snapshot.latest_updated_runs),
        )

    def _summary_panel(self, title: str, counts: dict[str, int]) -> Panel:
        table = Table.grid(padding=(0, 2))
        table.add_column(style="bold cyan")
        table.add_column(justify="right")
        if counts:
            for name, count in sorted(counts.items()):
                table.add_row(name, str(count))
        else:
            table.add_row("none", "0")
        return Panel(table, title=title, border_style="cyan")

    def _run_collection_panel(self, title: str, runs: Iterable[ObserverRunState]) -> Panel:
        table = Table(show_header=True, header_style="bold magenta", expand=True)
        table.add_column("Run")
        table.add_column("Audit")
        table.add_column("VUs", justify="right")
        table.add_column("Rep", justify="right")
        table.add_column("Status")
        table.add_column("Phase")
        table.add_column("Updated")
        rows_added = 0
        for run in runs:
            rows_added += 1
            table.add_row(
                str(run.run_id),
                run.audit_mode,
                str(run.virtual_users),
                str(run.repetition),
                run.status,
                run.phase,
                self._display_time(run.updated_at),
            )
        if rows_added == 0:
            table.add_row("-", "-", "-", "-", "-", "-", "-")
        return Panel(table, title=title, border_style="blue")

    def _failure_collection_panel(
        self,
        title: str,
        runs: Iterable[ObserverRunState],
        highlight_selection: bool = False,
    ) -> Panel:
        table = Table(show_header=True, header_style="bold red", expand=True)
        table.add_column("Run")
        table.add_column("Status")
        table.add_column("Phase")
        table.add_column("Audit")
        table.add_column("VUs", justify="right")
        table.add_column("Rep", justify="right")
        table.add_column("Error Type")
        table.add_column("Error Message")
        table.add_column("Updated")
        rows_added = 0
        for run in runs:
            rows_added += 1
            style = ""
            if highlight_selection and run.run_id == self.selected_run_id:
                style = "black on bright_cyan"
            table.add_row(
                str(run.run_id),
                run.status,
                run.phase,
                run.audit_mode,
                str(run.virtual_users),
                str(run.repetition),
                run.latest_error_type or "-",
                run.latest_error_message or "-",
                self._display_time(run.updated_at),
                style=style,
            )
        if rows_added == 0:
            table.add_row("-", "-", "-", "-", "-", "-", "-", "No triage candidates", "-")
        return Panel(table, title=title, border_style="red")

    def _render_runs_table(self):
        snapshot = self.snapshot
        if snapshot is None:
            return Panel("No runs loaded.", border_style="yellow")
        table = Table(show_header=True, header_style="bold magenta", expand=True)
        table.add_column("Run")
        table.add_column("Benchmark")
        table.add_column("Audit")
        table.add_column("VUs", justify="right")
        table.add_column("Rep", justify="right")
        table.add_column("Status")
        table.add_column("Phase")
        table.add_column("TPM", justify="right")
        table.add_column("NOPM", justify="right")
        table.add_column("Updated")
        for run in snapshot.runs:
            style = "black on bright_cyan" if run.run_id == self.selected_run_id else ""
            table.add_row(
                str(run.run_id),
                run.benchmark_name,
                run.audit_mode,
                str(run.virtual_users),
                str(run.repetition),
                run.status,
                run.phase,
                self._display_scalar(run.workload_metrics.get("tpm")),
                self._display_scalar(run.workload_metrics.get("nopm")),
                self._display_time(run.updated_at),
                style=style,
            )
        if not snapshot.runs:
            table.add_row("-", "-", "-", "-", "-", "-", "-", "-", "-", "-")
        return Panel(
            table,
            title="Runs",
            subtitle="Use arrows to select, Enter to inspect, :open <run_id> to jump directly",
            border_style="green",
        )

    def _render_failures_table(self):
        snapshot = self.snapshot
        if snapshot is None:
            return Panel("No runs loaded.", border_style="yellow")
        table = self._failure_collection_panel(
            "Failure Triage",
            snapshot.failure_runs,
            highlight_selection=True,
        ).renderable
        return Panel(
            table,
            title="Failure Triage",
            subtitle="Use arrows to select, Enter to open triage detail, :failures to jump here",
            border_style="red",
        )

    def _render_run_detail(self):
        if self.detail_mode == "triage":
            return self._render_failure_detail()
        return self._render_general_detail()

    def _render_general_detail(self):
        run = self._selected_run()
        if run is None:
            return Panel("No run selected. Use :runs to open the run list.", border_style="yellow")
        sections = [
            self._overview_panel(run),
            self._metrics_panel("Workload Metrics", run.workload_metrics),
            self._metrics_panel("Host Metrics", run.host_metrics),
            self._json_panel("Summary Metadata", run.summary_metadata),
            self._errors_panel(run),
            self._host_samples_panel(run),
            self._artifacts_panel(run, run.artifacts),
        ]
        return Group(*sections)

    def _render_failure_detail(self):
        run = self._selected_run()
        if run is None:
            return Panel("No failing run selected. Use :failures to open triage.", border_style="yellow")
        sections = [
            self._overview_panel(run),
            self._latest_error_panel(run),
            self._errors_panel(run, title="Persisted Errors"),
            self._artifacts_panel(
                run,
                run.triage_artifacts,
                title="Key Artifacts",
                subtitle=(
                    "Enter opens the selected artifact in selectable text mode. "
                    "Press e for the latest error text or s for a synthesized failure summary."
                ),
            ),
            self._metrics_panel("Workload Metrics", run.workload_metrics),
            self._metrics_panel("Host Metrics", run.host_metrics),
            self._host_samples_panel(run),
            self._json_panel("Summary Metadata", run.summary_metadata),
        ]
        return Group(*sections)

    def _render_text_preview(self):
        preview = self.text_preview
        if preview is None:
            return Panel("No text preview is open.", border_style="yellow")
        header_lines = [
            preview.title,
            "Selectable text mode: use normal terminal selection and copy.",
            "Esc returns to the previous view.",
        ]
        if preview.artifact is not None:
            header_lines.append(f"Artifact Type: {preview.artifact.artifact_type}")
        if preview.resolved_path is not None:
            header_lines.append(f"Path: {preview.resolved_path}")
        body = preview.text if preview.previewable else (preview.reason or "Preview is empty.")
        return Group(
            Text("\n".join(header_lines), style="bold cyan"),
            Text(""),
            Text(body, no_wrap=True, overflow="ignore"),
        )

    def _render_help(self):
        help_text = "\n".join(
            [
                "Views",
                "  dashboard: default summary view",
                "  :runs: open the full run table",
                "  :failures: open the failure triage table",
                "  Enter: runs -> detail, failures -> triage detail, detail -> text viewer",
                "  Esc / Left / Backspace: step back",
                "",
                "Text Viewer",
                "  Uses terminal-native selection and copy; there is no system clipboard integration.",
                "  Wrapped lines stay off by default so artifacts and errors copy cleanly.",
                "",
                "Shortcuts",
                "  : start command mode",
                "  ? open this help screen",
                "  r force an immediate reload",
                "  Up / Down move the current selection in runs, failures, and artifact lists",
                "  e open the latest persisted error as selectable text from detail",
                "  s open a synthesized failure summary as selectable text from detail",
                "",
                "Commands",
                "  :q",
                "  :runs",
                "  :failures",
                "  :dashboard",
                "  :help",
                "  :reload",
                "  :refresh 5",
                "  :open 12",
            ]
        )
        return Panel(help_text, title="Shortcuts and Commands", border_style="magenta")

    def _overview_panel(self, run: ObserverRunState) -> Panel:
        table = Table.grid(padding=(0, 2))
        table.add_column(style="bold cyan")
        table.add_column()
        table.add_row("Run", str(run.run_id))
        table.add_row("Benchmark", run.benchmark_name)
        table.add_row("Workload", f"{run.workload_name} ({run.workload_tool})")
        table.add_row("Audit", run.audit_mode)
        table.add_row("VUs / Rep", f"{run.virtual_users} / {run.repetition}")
        table.add_row("Status / Phase", f"{run.status} / {run.phase}")
        table.add_row("Created", self._display_time(run.created_at))
        table.add_row("Updated", self._display_time(run.updated_at))
        table.add_row("Output Dir", str(run.output_dir))
        if run.summary_notes:
            table.add_row("Notes", run.summary_notes)
        return Panel(table, title=f"Run {run.run_id} Overview", border_style="green")

    def _metrics_panel(self, title: str, metrics: dict[str, object]) -> Panel:
        table = Table.grid(padding=(0, 2))
        table.add_column(style="bold cyan")
        table.add_column()
        if metrics:
            for key, value in sorted(metrics.items()):
                table.add_row(key, self._display_scalar(value))
        else:
            table.add_row("none", "not available")
        return Panel(table, title=title, border_style="blue")

    def _json_panel(self, title: str, payload: dict[str, object]) -> Panel:
        if not payload:
            return Panel("not available", title=title, border_style="blue")
        return Panel(
            Text(json.dumps(payload, indent=2, sort_keys=True, default=str)),
            title=title,
            border_style="blue",
        )

    def _latest_error_panel(self, run: ObserverRunState) -> Panel:
        latest_error = run.latest_error
        if latest_error is None:
            return Panel(
                "No persisted errors are available for this run. Press s for a synthesized summary instead.",
                title="Latest Error",
                border_style="red",
            )
        table = Table.grid(padding=(0, 2))
        table.add_column(style="bold red")
        table.add_column()
        table.add_row("Phase", latest_error.phase)
        table.add_row("Type", latest_error.exception_type or "-")
        table.add_row("Created", self._display_time(latest_error.created_at))
        table.add_row("Message", latest_error.message or "-")
        return Panel(
            table,
            title="Latest Error",
            subtitle="Press e to open as selectable text",
            border_style="red",
        )

    def _errors_panel(self, run: ObserverRunState, title: str = "Errors") -> Panel:
        table = Table(show_header=True, header_style="bold red", expand=True)
        table.add_column("Phase")
        table.add_column("Type")
        table.add_column("Message")
        table.add_column("Created")
        if run.errors:
            for error in run.errors:
                table.add_row(
                    error.phase,
                    error.exception_type or "-",
                    error.message or "-",
                    self._display_time(error.created_at),
                )
        else:
            table.add_row("-", "-", "No persisted errors", "-")
        return Panel(table, title=title, border_style="red")

    def _host_samples_panel(self, run: ObserverRunState) -> Panel:
        table = Table(show_header=True, header_style="bold magenta", expand=True)
        table.add_column("#", justify="right")
        table.add_column("Timestamp")
        table.add_column("Total CPU", justify="right")
        table.add_column("SQL CPU", justify="right")
        table.add_column("Mem Used %", justify="right")
        table.add_column("SQL WS MB", justify="right")
        if run.host_samples:
            for sample in run.host_samples:
                table.add_row(
                    str(sample.sample_index),
                    sample.timestamp,
                    self._display_scalar(sample.total_cpu_percent),
                    self._display_scalar(sample.sql_cpu_percent),
                    self._display_scalar(sample.memory_used_percent),
                    self._display_scalar(sample.sql_working_set_mb),
                )
        else:
            table.add_row("-", "-", "-", "-", "-", "-")
        return Panel(table, title="Host Samples", border_style="magenta")

    def _artifacts_panel(
        self,
        run: ObserverRunState,
        artifacts: Iterable,
        title: str = "Artifacts",
        subtitle: str = "Use arrows to select an artifact, Enter to preview text artifacts",
    ) -> Panel:
        artifact_rows = tuple(artifacts)
        table = Table(show_header=True, header_style="bold yellow", expand=True)
        table.add_column("#", justify="right")
        table.add_column("Type")
        table.add_column("Path")
        table.add_column("Description")
        table.add_column("Created")
        if artifact_rows:
            for index, artifact in enumerate(artifact_rows):
                style = "black on bright_cyan" if index == self.selected_artifact_index else ""
                table.add_row(
                    str(index + 1),
                    artifact.artifact_type,
                    str(artifact.path),
                    artifact.description or "-",
                    self._display_time(artifact.created_at),
                    style=style,
                )
        else:
            table.add_row("-", "-", "-", "No artifacts", "-")
        return Panel(table, title=title, subtitle=subtitle, border_style="yellow")

    def _move_run_selection(self, delta: int) -> None:
        runs = self._run_collection_for_view()
        if not runs:
            return
        current_index = 0
        for index, run in enumerate(runs):
            if run.run_id == self.selected_run_id:
                current_index = index
                break
        next_index = max(0, min(len(runs) - 1, current_index + delta))
        self.selected_run_id = runs[next_index].run_id
        self.selected_artifact_index = 0
        label = "failure run" if self.view_mode == "failures" else "run"
        self.status_message = f"Selected {label} {self.selected_run_id}"
        self._render()

    def _move_artifact_selection(self, delta: int) -> None:
        run = self._selected_run()
        artifacts = self._selected_artifacts(run)
        if run is None or not artifacts:
            return
        next_index = max(0, min(len(artifacts) - 1, self.selected_artifact_index + delta))
        self.selected_artifact_index = next_index
        self.status_message = f"Selected artifact {self.selected_artifact_index + 1} for run {run.run_id}"
        self._render()

    def _enter_current_selection(self) -> bool:
        if self.view_mode == "runs":
            run = self._selected_run()
            if run is None:
                return False
            self._open_run_detail(run, detail_mode="general", return_view="runs")
            return True
        if self.view_mode == "failures":
            run = self._selected_run()
            if run is None:
                return False
            self._open_run_detail(run, detail_mode="triage", return_view="failures")
            return True
        if self.view_mode == "detail":
            run = self._selected_run()
            artifacts = self._selected_artifacts(run)
            if run is None:
                return False
            if artifacts:
                preview = self.service.preview_artifact_entry(
                    run,
                    artifacts[self.selected_artifact_index],
                    self.session_config,
                )
                self._open_text_preview(preview, f"Opened artifact text for run {run.run_id}")
                return True
            if self.detail_mode == "triage":
                return self._open_latest_error_preview()
        return False

    def _open_run_detail(self, run: ObserverRunState, detail_mode: str, return_view: str) -> None:
        self.selected_run_id = run.run_id
        self.detail_mode = detail_mode
        self.detail_return_view = return_view
        self.selected_artifact_index = 0
        self.text_preview = None
        self.view_mode = "detail"
        if detail_mode == "triage":
            self.status_message = f"Opened failure triage for run {run.run_id}"
        else:
            self.status_message = f"Opened run {run.run_id}"
        self._render()
        self._content_scroll().scroll_home(animate=False, immediate=True)

    def _open_text_preview(self, preview: TextPreview, message: str) -> None:
        self.text_preview = preview
        self.view_mode = "text"
        self.status_message = message
        self._render()
        self._content_scroll().scroll_home(animate=False, immediate=True)

    def _open_latest_error_preview(self) -> bool:
        run = self._selected_run()
        if run is None:
            return False
        preview = self.service.preview_latest_error(run)
        self._open_text_preview(preview, f"Opened latest error text for run {run.run_id}")
        return True

    def _open_failure_summary_preview(self) -> bool:
        run = self._selected_run()
        if run is None:
            return False
        preview = self.service.preview_failure_summary(run)
        self._open_text_preview(preview, f"Opened failure summary text for run {run.run_id}")
        return True

    def _navigate_back(self) -> bool:
        if self.view_mode == "text":
            self.view_mode = "detail"
            self.text_preview = None
            self.status_message = "Returned to run detail"
            self._render()
            return True
        if self.view_mode == "detail":
            self.view_mode = self.detail_return_view
            self._reconcile_selection()
            if self.detail_return_view == "failures":
                self.status_message = "Returned to failure triage"
            else:
                self.status_message = "Returned to run list"
            self._render()
            return True
        if self.view_mode in {"runs", "failures"}:
            self.view_mode = "dashboard"
            self._reconcile_selection()
            self.status_message = "Returned to dashboard"
            self._render()
            return True
        if self.view_mode == "help":
            self.view_mode = self.return_view
            self._reconcile_selection()
            self.status_message = "Closed help"
            self._render()
            return True
        return False

    def _toggle_help(self) -> None:
        if self.view_mode == "help":
            self.view_mode = self.return_view
            self._reconcile_selection()
            self.status_message = "Closed help"
        else:
            self.return_view = self.view_mode
            self.view_mode = "help"
            self.status_message = "Opened help"
        self._render()

    def _run_command(self, command_text: str) -> None:
        try:
            command = parse_command(command_text)
        except CommandParseError as exc:
            self.status_message = str(exc)
            self._render()
            return
        self._execute_command(command)

    def _execute_command(self, command: ObserverCommand) -> None:
        if command.name == "quit":
            self.exit()
            return
        if command.name == "runs":
            self.view_mode = "runs"
            self.text_preview = None
            self._reconcile_selection()
            self.status_message = "Opened run list"
            self._render()
            return
        if command.name == "failures":
            self.view_mode = "failures"
            self.text_preview = None
            self._reconcile_selection()
            self.status_message = "Opened failure triage"
            self._render()
            return
        if command.name == "dashboard":
            self.view_mode = "dashboard"
            self.text_preview = None
            self._reconcile_selection()
            self.status_message = "Opened dashboard"
            self._render()
            return
        if command.name == "help":
            self.return_view = self.view_mode
            self.view_mode = "help"
            self.status_message = "Opened help"
            self._render()
            return
        if command.name == "reload":
            self._refresh_snapshot("Manual refresh")
            return
        if command.name == "refresh":
            self.refresh_seconds = float(command.value or self.refresh_seconds)
            self._restart_refresh_timer()
            self.status_message = f"Refresh interval set to {self.refresh_seconds:g}s"
            self._render()
            return
        if command.name == "open":
            if self.snapshot is None:
                self.status_message = "Snapshot is not available yet"
                self._render()
                return
            run = self.snapshot.find_run(int(command.value))
            if run is None:
                self.status_message = f"Run {command.value} not found"
                self._render()
                return
            self._open_run_detail(run, detail_mode="general", return_view="runs")

    def _selected_run(self) -> ObserverRunState | None:
        if self.snapshot is None or self.selected_run_id is None:
            return None
        return self.snapshot.find_run(self.selected_run_id)

    def _display_time(self, value: str) -> str:
        try:
            return datetime.fromisoformat(value).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            return value

    def _display_scalar(self, value: object) -> str:
        if value is None or value == "":
            return "-"
        if isinstance(value, float):
            return f"{value:.2f}"
        return str(value)


def launch_observer_app(session_config: ObserverSessionConfig) -> None:
    BenchpressObserverApp(session_config).run()
