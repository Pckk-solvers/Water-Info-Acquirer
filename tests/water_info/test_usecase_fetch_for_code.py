import pytest

from src.water_info.domain.models import Options, Period, WaterInfoRequest
from src.water_info.service.usecase import fetch_for_code


def test_fetch_for_code_returns_result_on_success():
    period = Period(year_start="2024", year_end="2024", month_start="1月", month_end="1月")
    request = WaterInfoRequest(period=period, mode_type="S", options=Options(use_daily=False, single_sheet=False))

    def _hourly(*args, **kwargs):
        return "out.xlsx"

    outcome = fetch_for_code(
        code="123",
        request=request,
        fetch_hourly=_hourly,
        fetch_daily=lambda *a, **k: "",
    )

    assert outcome.error is None
    assert outcome.result is not None
    assert outcome.result.file_path == "out.xlsx"


def test_fetch_for_code_returns_error_on_failure():
    period = Period(year_start="2024", year_end="2024", month_start="1月", month_end="1月")
    request = WaterInfoRequest(period=period, mode_type="S", options=Options(use_daily=False, single_sheet=False))

    def _hourly(*args, **kwargs):
        raise RuntimeError("boom")

    outcome = fetch_for_code(
        code="999",
        request=request,
        fetch_hourly=_hourly,
        fetch_daily=lambda *a, **k: "",
    )

    assert outcome.result is None
    assert outcome.error is not None
    assert outcome.error.code == "999"
    assert outcome.error.error_type == "RuntimeError"
    assert outcome.error.message == "boom"
