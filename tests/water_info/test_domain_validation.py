import pytest

from src.water_info.domain.models import Options, Period, WaterInfoRequest


def test_period_rejects_invalid_year():
    with pytest.raises(ValueError):
        Period(year_start="20A4", year_end="2024", month_start="1月", month_end="1月")


def test_period_rejects_invalid_month_format():
    with pytest.raises(ValueError):
        Period(year_start="2024", year_end="2024", month_start="1", month_end="1月")


def test_period_rejects_out_of_range_month():
    with pytest.raises(ValueError):
        Period(year_start="2024", year_end="2024", month_start="13月", month_end="1月")


def test_period_rejects_reverse_range():
    with pytest.raises(ValueError):
        Period(year_start="2024", year_end="2023", month_start="2月", month_end="1月")


def test_request_rejects_invalid_mode():
    period = Period(year_start="2024", year_end="2024", month_start="1月", month_end="1月")
    options = Options(use_daily=False, single_sheet=False)
    with pytest.raises(ValueError):
        WaterInfoRequest(period=period, mode_type="X", options=options)
