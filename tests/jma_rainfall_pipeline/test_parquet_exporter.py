from datetime import date

import pandas as pd

from src.jma_rainfall_pipeline.exporter import parquet_exporter


def test_export_weather_parquet_sets_station_name(monkeypatch, tmp_path):
    monkeypatch.setattr(
        parquet_exporter,
        "fetch_station_codes",
        lambda _prec: [{"block_no": "47401", "station": "さいたま"}],
    )
    parquet_exporter._STATION_CACHE.clear()

    df = pd.DataFrame(
        {
            "datetime": ["2024-01-01 01:00:00", "2024-01-01 02:00:00"],
            "precipitation": [1.2, 0.0],
        }
    )

    out = parquet_exporter.export_weather_parquet(
        df,
        prec_no="11",
        block_no="47401",
        interval_label="hourly",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 31),
        output_dir=tmp_path,
    )

    saved = pd.read_parquet(out, engine="pyarrow")
    assert out.exists()
    assert set(saved["station_name"].unique()) == {"さいたま"}
    assert pd.Timestamp(saved.loc[0, "observed_at"]) == pd.Timestamp("2024-01-01 01:00:00")
    assert pd.Timestamp(saved.loc[1, "observed_at"]) == pd.Timestamp("2024-01-01 02:00:00")
    assert pd.Timestamp(saved.loc[0, "period_start_at"]) == pd.Timestamp("2024-01-01 00:00:00")
    assert pd.Timestamp(saved.loc[0, "period_end_at"]) == pd.Timestamp("2024-01-01 01:00:00")


def test_export_weather_parquet_normalizes_legacy_midnight(monkeypatch, tmp_path):
    monkeypatch.setattr(
        parquet_exporter,
        "fetch_station_codes",
        lambda _prec: [{"block_no": "47401", "station": "さいたま"}],
    )
    parquet_exporter._STATION_CACHE.clear()

    df = pd.DataFrame(
        {
            "datetime": ["2026-02-01 23:59:59.999999"],
            "precipitation": [0.0],
        }
    )

    out = parquet_exporter.export_weather_parquet(
        df,
        prec_no="11",
        block_no="47401",
        interval_label="hourly",
        start_date=date(2026, 2, 1),
        end_date=date(2026, 2, 1),
        output_dir=tmp_path,
    )

    saved = pd.read_parquet(out, engine="pyarrow")
    assert out.exists()
    assert pd.Timestamp(saved.loc[0, "observed_at"]) == pd.Timestamp("2026-02-02 00:00:00")
    assert pd.Timestamp(saved.loc[0, "period_start_at"]) == pd.Timestamp("2026-02-01 23:00:00")
