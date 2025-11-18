from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, Sequence

from src.core.fetch import FetchResponse


@dataclass(frozen=True)
class ExportOptions:
    single_sheet: bool
    template: str


@dataclass(frozen=True)
class ExportRequest:
    response: FetchResponse
    options: ExportOptions

    @property
    def records(self):
        return self.response.records


@dataclass(frozen=True)
class ExportResult:
    file_path: Path


class WorkbookComposer(Protocol):
    def compose(self, request: ExportRequest) -> Path:
        ...


class ExportService:
    def __init__(self, composer: WorkbookComposer) -> None:
        self._composer = composer

    def export(self, response: FetchResponse, options: ExportOptions) -> ExportResult:
        if not response.records:
            raise ValueError("No records to export")
        request = ExportRequest(response=response, options=options)
        file_path = self._composer.compose(request)
        return ExportResult(file_path=file_path)
