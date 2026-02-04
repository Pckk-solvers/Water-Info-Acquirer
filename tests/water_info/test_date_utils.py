from datetime import datetime

from src.water_info.main_datetime import month_floor, shift_month


def test_month_floor_returns_month_start():
    dt = datetime(2024, 5, 17, 12, 30)
    assert month_floor(dt) == datetime(2024, 5, 1)


def test_shift_month_across_year():
    dt = datetime(2024, 12, 1)
    assert shift_month(dt, 1) == datetime(2025, 1, 1)
    assert shift_month(dt, -1) == datetime(2024, 11, 1)
