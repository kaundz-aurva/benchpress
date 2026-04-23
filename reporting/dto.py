from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from reporting.constants import DEFAULT_CSV_DIRNAME


@dataclass(frozen=True)
class ReportGenerationRequest:
    db_path: Path
    markdown_path: Path
    html_path: Path | None = None
    csv_dir: Path | None = None
    artifact_root: Path | None = None
    include_artifact_fallback: bool = True

    def __post_init__(self) -> None:
        object.__setattr__(self, "db_path", Path(self.db_path))
        object.__setattr__(self, "markdown_path", Path(self.markdown_path))
        if self.html_path is not None:
            object.__setattr__(self, "html_path", Path(self.html_path))
        if self.csv_dir is not None:
            object.__setattr__(self, "csv_dir", Path(self.csv_dir))
        if self.artifact_root is not None:
            object.__setattr__(self, "artifact_root", Path(self.artifact_root))

    @property
    def resolved_csv_dir(self) -> Path:
        if self.csv_dir is not None:
            return self.csv_dir
        return self.markdown_path.parent / DEFAULT_CSV_DIRNAME

    @property
    def resolved_html_path(self) -> Path:
        if self.html_path is not None:
            return self.html_path
        return self.markdown_path.with_suffix(".html")

    @property
    def resolved_artifact_root(self) -> Path:
        if self.artifact_root is not None:
            return self.artifact_root
        return self.db_path.parent


@dataclass(frozen=True)
class ReportGenerationResult:
    markdown_path: Path
    html_path: Path
    csv_paths: dict[str, Path] = field(default_factory=dict)
    total_runs: int = 0
    successful_runs: int = 0
    non_successful_runs: int = 0

    def __post_init__(self) -> None:
        object.__setattr__(self, "markdown_path", Path(self.markdown_path))
        object.__setattr__(self, "html_path", Path(self.html_path))
        object.__setattr__(
            self,
            "csv_paths",
            {name: Path(path) for name, path in self.csv_paths.items()},
        )

    def as_dict(self) -> dict[str, object]:
        return {
            "markdown_path": str(self.markdown_path),
            "html_path": str(self.html_path),
            "csv_paths": {name: str(path) for name, path in self.csv_paths.items()},
            "total_runs": self.total_runs,
            "successful_runs": self.successful_runs,
            "non_successful_runs": self.non_successful_runs,
        }
