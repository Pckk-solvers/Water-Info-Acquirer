from __future__ import annotations

from datetime import datetime

from src.jma_rainfall_pipeline.api.datetime_bounds import normalize_payload_datetime


def test_normalize_payload_datetime_converts_plain_end_date_to_exclusive_midnight() -> None:
    start = normalize_payload_datetime("2026-03-01", field_name="start_date")
    end = normalize_payload_datetime("2026-03-03", field_name="end_date")

    assert start == datetime(2026, 3, 1, 0, 0, 0)
    assert end == datetime(2026, 3, 4, 0, 0, 0)


def test_normalize_payload_datetime_keeps_explicit_end_datetime() -> None:
    end = normalize_payload_datetime("2026-03-03T12:30:00", field_name="end_date")

    assert end == datetime(2026, 3, 3, 12, 30, 0)
