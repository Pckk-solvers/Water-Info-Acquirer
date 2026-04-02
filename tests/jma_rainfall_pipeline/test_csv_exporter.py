import pandas as pd

from src.jma_rainfall_pipeline.exporter import csv_exporter


def test_prepare_export_frame_hourly_uses_normalized_period_contract():
    df = pd.DataFrame(
        {
            "datetime": ["2026-02-01 23:59:59.999999", "2026-02-02 01:00:00"],
            "precipitation": [0.0, 1.2],
        }
    )

    prepared = csv_exporter._prepare_export_frame(df, "hourly")

    assert pd.Timestamp(prepared.loc[0, "observed_at"]) == pd.Timestamp("2026-02-02 00:00:00")
    assert pd.Timestamp(prepared.loc[0, "period_start_at"]) == pd.Timestamp("2026-02-01 23:00:00")
    assert prepared.loc[0, "hour"] == 0
    assert prepared.loc[0, "time"] == "00:00"
