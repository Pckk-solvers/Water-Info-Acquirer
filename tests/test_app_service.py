from datetime import date
from dataclasses import dataclass

import pytest

from src.core.app import AppService, ExecutionOptions, UseCaseResult


class DummyFetchService:
    def __init__(self, response):
        self.response = response
        self.last_request = None

    def fetch(self, request):
        self.last_request = request
        return self.response


class DummyExportService:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.last_call = None

    def export(self, response, options):
        self.last_call = (response, options)
        return UseCaseResult(file_path=self.file_path)


class DummyTelemetry:
    def __init__(self):
        self.events = []

    def emit_event(self, kind, **payload):
        self.events.append(kind)


class DummyValidator:
    def validate(self, options: ExecutionOptions) -> None:
        if not options.codes:
            raise ValueError("Codes required")


class DummyFetchResponse:
    def __init__(self, records):
        self.records = records


def base_options():
    return ExecutionOptions(
        codes=["123"],
        period_start=date(2024, 1, 1),
        period_end=date(2024, 2, 1),
        mode="S",
        single_sheet=True,
        use_daily=False,
    )


def test_app_service_execute_calls_services():
    fetch_response = DummyFetchResponse(records=[{"v": 1}])
    fetch_service = DummyFetchService(fetch_response)
    export_service = DummyExportService(file_path="out.xlsx")
    telemetry = DummyTelemetry()
    service = AppService(fetch_service, export_service, telemetry, DummyValidator())

    result = service.execute(base_options())

    assert result.file_path == "out.xlsx"
    assert telemetry.events == ["run.start", "run.success"]
    assert fetch_service.last_request.code == "123"
    assert export_service.last_call[0] is fetch_response


def test_app_service_validates_input():
    fetch_service = DummyFetchService(None)
    export_service = DummyExportService("dummy")
    telemetry = DummyTelemetry()
    service = AppService(fetch_service, export_service, telemetry, DummyValidator())

    bad_options = base_options()
    bad_options.codes = []

    with pytest.raises(ValueError):
        service.execute(bad_options)
