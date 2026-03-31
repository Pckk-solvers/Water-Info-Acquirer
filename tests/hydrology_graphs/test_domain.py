from __future__ import annotations

from datetime import date

import pandas as pd

from hydrology_graphs.domain.logic import (
    annual_max_by_year,
    annual_max_series,
    build_output_target,
    event_window_bounds,
    expected_event_index,
    has_min_years,
    threshold_key,
)
from hydrology_graphs.domain.models import GraphTarget


def test_event_window_bounds_are_centered():
    start, end = event_window_bounds(date(2025, 1, 2), 3)
    assert start.isoformat() == "2025-01-01T00:00:00"
    assert end.isoformat() == "2025-01-04T00:00:00"
    assert len(expected_event_index(date(2025, 1, 2), 3)) == 72


def test_annual_max_helpers_work():
    frame = pd.DataFrame(
        {
            "observed_at": [
                "2010-01-01 00:00:00",
                "2010-06-01 00:00:00",
                "2011-01-01 00:00:00",
                "2011-06-01 00:00:00",
            ],
            "value": [1.0, 3.0, 2.0, 5.0],
        }
    )
    series = annual_max_series(frame)
    assert series.loc[2010] == 3.0
    assert series.loc[2011] == 5.0
    assert has_min_years(series, 2)

    annual = annual_max_by_year(frame)
    assert annual["year"].tolist() == [2010, 2011]
    assert annual["value"].tolist() == [3.0, 5.0]


def test_threshold_key_and_output_target():
    target = GraphTarget(source="jma", station_key="111", graph_type="hyetograph", base_date=None)
    assert threshold_key("jma", "111", "hyetograph") == "jma|111|hyetograph"
    assert build_output_target(target) == "jma:111:hyetograph:annual"
