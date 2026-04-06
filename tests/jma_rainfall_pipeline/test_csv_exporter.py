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


def test_normalize_time_column_prioritizes_period_end_at() -> None:
    df = pd.DataFrame(
        {
            "period_end_at": ["2026-02-01 00:00:00"],
            "observed_at": ["2026-02-01 01:00:00"],
        }
    )

    normalized = csv_exporter._normalize_time_column(df)

    assert normalized is not None
    assert normalized.iloc[0] == "2026/02/01 00:00"


def test_export_precipitation_excel_creates_year_sheets_by_default(tmp_path):
    df = pd.DataFrame(
        {
            "observed_at": ["2025-12-31 23:00:00", "2026-01-01 00:00:00"],
            "precipitation": [1.0, 2.0],
        }
    )
    csv_path = tmp_path / "test.csv"
    overview = {"exported_at": "2026-04-06 10:00"}

    excel_path = csv_exporter._export_precipitation_excel(
        df,
        csv_path,
        excel_output_dir=tmp_path,
        overview_info=overview,
        include_all_period_sheet=False,
    )

    assert excel_path is not None
    with pd.ExcelFile(excel_path) as xls:
        assert "全期間" not in xls.sheet_names
        assert "2025年" in xls.sheet_names
        assert "2026年" in xls.sheet_names
        assert "出典" in xls.sheet_names


def test_export_precipitation_excel_adds_all_period_sheet_when_enabled(tmp_path):
    df = pd.DataFrame(
        {
            "observed_at": ["2025-12-31 23:00:00", "2026-01-01 00:00:00"],
            "precipitation": [1.0, 2.0],
        }
    )
    csv_path = tmp_path / "test.csv"
    overview = {"exported_at": "2026-04-06 10:00"}

    excel_path = csv_exporter._export_precipitation_excel(
        df,
        csv_path,
        excel_output_dir=tmp_path,
        overview_info=overview,
        include_all_period_sheet=True,
    )

    assert excel_path is not None
    with pd.ExcelFile(excel_path) as xls:
        assert "全期間" in xls.sheet_names
        assert "2025年" in xls.sheet_names
        assert "2026年" in xls.sheet_names
        assert "出典" in xls.sheet_names
