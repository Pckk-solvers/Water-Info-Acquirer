from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from river_meta.rainfall.sources.jma.adapter import _align_hourly_timestamp_to_waterinfo


def test_align_hourly_timestamp_shifts_back_one_hour():
    observed_at = datetime(2025, 1, 3, 0, 0, 0)
    aligned = _align_hourly_timestamp_to_waterinfo(observed_at, interval="1hour")
    assert aligned == datetime(2025, 1, 2, 23, 0, 0)


def test_align_hourly_timestamp_keeps_non_hourly():
    observed_at = datetime(2025, 1, 3, 0, 0, 0)
    aligned = _align_hourly_timestamp_to_waterinfo(observed_at, interval="1day")
    assert aligned == datetime(2025, 1, 3, 0, 0, 0)
