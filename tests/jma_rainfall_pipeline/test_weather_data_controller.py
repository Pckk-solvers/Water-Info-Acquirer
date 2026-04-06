from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd

from src.jma_rainfall_pipeline.controller import weather_data_controller
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
        pd.Timestamp("2026-02-02 00:00:00"),
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


def test_fetch_summary_passes_include_all_period_sheet_to_exporter(monkeypatch, tmp_path) -> None:
    controller = WeatherDataController(interval=timedelta(hours=1))
    start = datetime(2026, 2, 2, 0, 0, 0)
    end = datetime(2026, 2, 3, 0, 0, 0)
    captured: dict[str, object] = {}

    def _fake_schedule_fetch(station_list, fetch_start, fetch_end):
        return [(("11", "47401"), start, "<html>ok</html>", "https://example.com")]

    def _fake_parse_html(html, freq_label, target_date, obs_type):
        return pd.DataFrame({"datetime": ["2026-02-02 01:00:00"], "precipitation": [0.2]})

    def _fake_export_weather_data(*args, **kwargs):
        captured["include_all_period_sheet"] = kwargs.get("include_all_period_sheet")
        return Path(tmp_path / "out.xlsx")

    monkeypatch.setattr(controller.fetcher, "schedule_fetch", _fake_schedule_fetch)
    monkeypatch.setattr(weather_data_controller, "parse_html", _fake_parse_html)
    monkeypatch.setattr(weather_data_controller, "export_weather_data", _fake_export_weather_data)
    monkeypatch.setattr(
        controller,
        "_filter_dataframe_by_range",
        lambda merged_df, interval_label, filter_start, filter_end: merged_df,
    )

    summary = controller.fetch_and_export_summary(
        stations=[("11", "47401", "a1")],
        start=start,
        end=end,
        output_dir=tmp_path,
        export_csv=False,
        export_excel=True,
        include_all_period_sheet=True,
        export_parquet=False,
        export_ndjson=False,
    )

    assert len(summary.results) == 1
    assert captured["include_all_period_sheet"] is True
