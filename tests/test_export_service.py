from dataclasses import dataclass
from pathlib import Path

import pytest

from src.core.export import ExportOptions, ExportRequest, ExportResult, ExportService
from src.core.fetch import FetchResponse


@dataclass
class DummyComposer:
    output_path: Path

    def __post_init__(self):
        self.calls = []

    def compose(self, request: ExportRequest) -> Path:
        self.calls.append(request)
        return self.output_path


def build_response(records):
    return FetchResponse(
        code="123",
        station_name="Station",
        records=records,
        coverage=(None, None),
        mode="S",
    )


def test_export_single_sheet_calls_composer(tmp_path):
    composer = DummyComposer(tmp_path / "file.xlsx")
    service = ExportService(composer=composer)
    response = build_response([{"ts": 1, "value": 2}])
    options = ExportOptions(single_sheet=True, template="WH")

    result = service.export(response, options)

    assert result.file_path == composer.output_path
    assert composer.calls[0].options.single_sheet is True
    assert composer.calls[0].records == response.records


def test_export_raises_when_no_records(tmp_path):
    composer = DummyComposer(tmp_path / "empty.xlsx")
    service = ExportService(composer=composer)
    response = build_response([])
    options = ExportOptions(single_sheet=False, template="WH")

    with pytest.raises(ValueError):
        service.export(response, options)


def test_export_template_passed_to_composer(tmp_path):
    composer = DummyComposer(tmp_path / "file.xlsx")
    service = ExportService(composer=composer)
    response = build_response([{"ts": 1}])
    options = ExportOptions(single_sheet=False, template="QD")

    service.export(response, options)

    assert composer.calls[0].options.template == "QD"
