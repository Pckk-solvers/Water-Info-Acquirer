from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Protocol

from src.core.fetch import FetchRequest
from src.core.export import ExportOptions
from src.core.telemetry import TelemetryService


@dataclass
class ExecutionOptions:
    codes: list[str]
    period_start: date
    period_end: date
    mode: str
    single_sheet: bool
    use_daily: bool


@dataclass
class UseCaseResult:
    file_path: str


class FetchService(Protocol):
    def fetch(self, request: FetchRequest):
        ...


class ExportService(Protocol):
    def export(self, response, options: ExportOptions):
        ...


class OptionsValidator(Protocol):
    def validate(self, options: ExecutionOptions) -> None:
        ...


class AppService:
    def __init__(
        self,
        fetch_service: FetchService,
        export_service: ExportService,
        telemetry: TelemetryService,
        validator: OptionsValidator,
    ) -> None:
        self._fetch = fetch_service
        self._export = export_service
        self._telemetry = telemetry
        self._validator = validator

    def execute(self, options: ExecutionOptions) -> UseCaseResult:
        self._validator.validate(options)
        self._telemetry.emit_event("run.start", options=options)
        fetch_request = FetchRequest(
            code=options.codes[0],
            mode=options.mode,
            period_start=options.period_start,
            period_end=options.period_end,
        )
        fetch_response = self._fetch.fetch(fetch_request)
        export_options = ExportOptions(single_sheet=options.single_sheet, template="WH")
        result = self._export.export(fetch_response, export_options)
        self._telemetry.emit_event("run.success", file_path=result.file_path)
        return result
