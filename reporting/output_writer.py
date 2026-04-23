from __future__ import annotations

from reporting.constants import SUCCESS_STATUS
from reporting.csv_exporter import CsvReportExporter
from reporting.dto import ReportGenerationRequest, ReportGenerationResult
from reporting.html_renderer import HtmlReportRenderer
from reporting.markdown_renderer import MarkdownReportRenderer
from reporting.models import ReportDocument


class ReportOutputWriter:
    def __init__(
        self,
        csv_exporter: CsvReportExporter | None = None,
        markdown_renderer: MarkdownReportRenderer | None = None,
        html_renderer: HtmlReportRenderer | None = None,
    ) -> None:
        self.csv_exporter = csv_exporter or CsvReportExporter()
        self.markdown_renderer = markdown_renderer or MarkdownReportRenderer()
        self.html_renderer = html_renderer or HtmlReportRenderer()

    def write(
        self,
        request: ReportGenerationRequest,
        document: ReportDocument,
    ) -> ReportGenerationResult:
        request.markdown_path.parent.mkdir(parents=True, exist_ok=True)
        html_path = request.resolved_html_path
        html_path.parent.mkdir(parents=True, exist_ok=True)
        csv_dir = request.resolved_csv_dir
        csv_dir.mkdir(parents=True, exist_ok=True)

        csv_paths = self.csv_exporter.write(document, csv_dir)
        request.markdown_path.write_text(
            self.markdown_renderer.render(document, csv_paths),
            encoding="utf-8",
        )
        html_path.write_text(
            self.html_renderer.render(document, csv_paths),
            encoding="utf-8",
        )

        successful_runs = sum(1 for run in document.runs if run.status == SUCCESS_STATUS)
        return ReportGenerationResult(
            markdown_path=request.markdown_path,
            html_path=html_path,
            csv_paths=csv_paths,
            total_runs=len(document.runs),
            successful_runs=successful_runs,
            non_successful_runs=len(document.runs) - successful_runs,
        )
