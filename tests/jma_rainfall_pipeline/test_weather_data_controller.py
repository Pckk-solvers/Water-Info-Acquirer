from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd

from src.jma_rainfall_pipeline.controller.weather_data_controller import WeatherDataController


def test_filter_dataframe_for_parquet_hourly_uses_shifted_window() -> None:
    controller = WeatherDataController(interval=timedelta(hours=1))
    start = datetime(2026, 2, 2, 0, 0, 0)
    end = datetime(2026, 2, 2, 23, 59, 59, 999999)

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

    filtered = controller._filter_dataframe_for_parquet(df, "hourly", start, end)

    actual = pd.to_datetime(filtered["datetime"]).tolist()
    assert actual == [
        pd.Timestamp("2026-02-02 01:00:00"),
        pd.Timestamp("2026-02-02 23:00:00"),
        pd.Timestamp("2026-02-03 00:00:00"),
    ]

