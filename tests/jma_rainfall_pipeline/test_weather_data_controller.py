from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd

from src.jma_rainfall_pipeline.controller.weather_data_controller import WeatherDataController


def test_filter_dataframe_by_range_hourly_uses_period_end_window() -> None:
    controller = WeatherDataController(interval=timedelta(hours=1))
    start = datetime(2026, 2, 2, 0, 0, 0)
    end = datetime(2026, 2, 3, 0, 0, 0)

    df = pd.DataFrame(
        {
            "datetime": [
                "2026-02-02 00:00:00",
                "2026-02-02 01:00:00",
                "2026-02-02 23:00:00",
                "2026-02-03 00:00:00",
                "2026-02-03 01:00:00",
            ],
            "precipitation": [0.1, 0.2, 0.3, 0.4, 0.5],
        }
    )

    filtered = controller._filter_dataframe_by_range(df, "hourly", start, end)

    actual = pd.to_datetime(filtered["datetime"]).tolist()
    assert actual == [
        pd.Timestamp("2026-02-02 01:00:00"),
        pd.Timestamp("2026-02-02 23:00:00"),
        pd.Timestamp("2026-02-03 00:00:00"),
    ]


def test_collect_mergeable_frames_skips_all_na_frame() -> None:
    controller = WeatherDataController(interval=timedelta(hours=1))

    usable = pd.DataFrame({"datetime": ["2026-02-02 01:00:00"], "precipitation": [0.2]})
    all_na = pd.DataFrame({"datetime": [None], "precipitation": [None]})

    mergeable = controller._collect_mergeable_frames([usable, all_na])

    assert len(mergeable) == 1
    assert mergeable[0].equals(usable)
