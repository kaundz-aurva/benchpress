# Observer Context

This folder owns the client-VM terminal observer for live Benchpress runs.

Responsibilities:

- Read run state from the existing SQLite database without mutating it.
- Reuse reporting-side run/artifact/error joins where practical.
- Provide dashboard and drill-down data for the terminal UI.
- Resolve and preview safe text artifacts under a trusted artifact root.

Rules:

- Stay read-only with respect to `benchpress.sqlite3` and run artifacts.
- Keep Textual-specific UI code in `ui.py`; keep data loading and command parsing framework-neutral.
- Prefer existing reporting models and helpers over inventing a parallel persistence model.
- Do not make the observer responsible for launching or controlling benchmark runs in this slice.
