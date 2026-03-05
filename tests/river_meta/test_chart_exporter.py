from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from river_meta.rainfall.analysis import build_annual_max_dataframe, build_hourly_timeseries_dataframe
from river_meta.rainfall.chart_exporter import (
    METRICS,
    _compute_before_after,
    compute_chart_config,
    export_rainfall_charts,
)


# ---------------------------------------------------------------------------
# ヘルパー
# ---------------------------------------------------------------------------


def _raw_records(values: list[tuple[str, float | None]], *, station_key: str = "47401", station_name: str = "さいたま") -> pd.DataFrame:
    rows = []
    for ts, rainfall in values:
        rows.append(
            {
                "source": "jma",
                "station_key": station_key,
                "station_name": station_name,
                "observed_at": ts,
                "interval": "1hour",
                "rainfall_mm": rainfall,
                "quality": "normal" if rainfall is not None else "missing",
            }
        )
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# CHART_CONFIG 算出テスト
# ---------------------------------------------------------------------------


def test_compute_before_after_1hour_is_symmetric():
    before, after = _compute_before_after("1時間雨量")
    total = before + after
    assert total == timedelta(hours=24)
    assert before == after  # 1時間は比率1:1


def test_compute_before_after_48hour_is_3_to_1():
    before, after = _compute_before_after("48時間雨量")
    total = before + after
    assert total == timedelta(hours=96)
    assert before == timedelta(hours=72)
    assert after == timedelta(hours=24)


def test_compute_chart_config_returns_expected_keys():
    for metric in METRICS:
        config = compute_chart_config(metric)
        assert "metric" in config
        assert "before" in config
        assert "after" in config
        assert "total_hours" in config
        assert "label_interval" in config
        assert config["before"] + config["after"] == timedelta(hours=config["total_hours"])


def test_before_is_always_gte_after():
    """前（before）は常に後（after）以上であること。"""
    for metric in METRICS:
        config = compute_chart_config(metric)
        assert config["before"] >= config["after"], f"{metric}: before < after"


# ---------------------------------------------------------------------------
# PNG 生成テスト
# ---------------------------------------------------------------------------


def _make_72h_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """3日分のダミー時系列データを作る。"""
    import numpy as np

    np.random.seed(42)
    hours = pd.date_range("2025-01-01 00:00", periods=72, freq="h")
    rainfall = np.random.exponential(2.0, size=72).round(1)
    # ピークを作る
    rainfall[36] = 30.0

    rows = []
    for dt, rain in zip(hours, rainfall):
        rows.append(
            {
                "source": "jma",
                "station_key": "47401",
                "station_name": "テスト観測所",
                "observed_at": dt.isoformat(),
                "interval": "1hour",
                "rainfall_mm": float(rain),
                "quality": "normal",
            }
        )
    source_df = pd.DataFrame(rows)
    timeseries_df = build_hourly_timeseries_dataframe(source_df)
    annual_max_df = build_annual_max_dataframe(timeseries_df)
    return timeseries_df, annual_max_df


def test_export_rainfall_charts_creates_png_files(tmp_path):
    timeseries_df, annual_max_df = _make_72h_data()
    output_dir = tmp_path / "charts"

    paths = export_rainfall_charts(
        timeseries_df,
        annual_max_df,
        output_dir=str(output_dir),
        station_key="47401",
        station_name="テスト観測所",
    )

    assert len(paths) > 0
    for p in paths:
        assert p.exists()
        assert p.suffix == ".png"
        # PNG header check
        with open(p, "rb") as f:
            header = f.read(8)
            assert header[:4] == b"\x89PNG"


def test_export_rainfall_charts_empty_data_returns_empty(tmp_path):
    empty = pd.DataFrame()
    paths = export_rainfall_charts(
        empty,
        empty,
        output_dir=str(tmp_path / "empty"),
        station_key="99999",
    )
    assert paths == []


def test_export_rainfall_charts_empty_annual_max(tmp_path):
    source_df = _raw_records([("2025-01-01 00:00:00", 1.0)])
    timeseries_df = build_hourly_timeseries_dataframe(source_df)
    empty_annual = pd.DataFrame()

    paths = export_rainfall_charts(
        timeseries_df,
        empty_annual,
        output_dir=str(tmp_path / "no_annual"),
        station_key="47401",
    )
    assert paths == []


def test_export_rainfall_charts_respects_should_stop(tmp_path):
    timeseries_df, annual_max_df = _make_72h_data()
    output_dir = tmp_path / "charts_cancel"

    paths = export_rainfall_charts(
        timeseries_df,
        annual_max_df,
        output_dir=str(output_dir),
        station_key="47401",
        station_name="テスト観測所",
        should_stop=lambda: True,
    )

    assert paths == []
