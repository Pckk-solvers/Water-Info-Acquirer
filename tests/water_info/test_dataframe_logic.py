import pandas as pd
import pytest

from src.water_info import entry


def test_process_data_for_code_empty_data_raises(fake_bs4, fake_station_payload, make_values_payload, fake_throttled_get_factory, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    payloads = [fake_station_payload, make_values_payload(["x"])]
    monkeypatch.setattr(entry, "throttled_get", fake_throttled_get_factory(payloads))
    with pytest.raises(entry.EmptyExcelWarning):
        entry.process_data_for_code(
            code="123",
            Y1="2024",
            Y2="2024",
            M1="1月",
            M2="1月",
            mode_type="R",
            single_sheet=False,
        )


def test_process_period_date_display_for_code_empty_data_raises(fake_bs4, fake_station_payload, make_values_payload, fake_throttled_get_factory, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    payloads = [fake_station_payload, make_values_payload(["x"])]
    monkeypatch.setattr(entry, "throttled_get", fake_throttled_get_factory(payloads))
    with pytest.raises(entry.EmptyExcelWarning):
        entry.process_period_date_display_for_code(
            code="456",
            Y1="2024",
            Y2="2024",
            M1="1月",
            M2="1月",
            mode_type="S",
            single_sheet=False,
        )


def test_process_data_for_code_display_dt_and_sheet_year(fake_bs4, fake_station_payload, make_values_payload, fake_throttled_get_factory, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    values = [str(i) for i in range(1, 24 * 31 + 1)]
    payloads = [fake_station_payload, make_values_payload(values)]
    monkeypatch.setattr(entry, "throttled_get", fake_throttled_get_factory(payloads))

    file_path = entry.process_data_for_code(
        code="789",
        Y1="2024",
        Y2="2024",
        M1="12月",
        M2="12月",
        mode_type="R",
        single_sheet=True,
    )

    xls = pd.ExcelFile(file_path)
    assert "2024年" in xls.sheet_names
    assert "summary" in xls.sheet_names
    assert "全期間" in xls.sheet_names

    df = pd.read_excel(file_path, sheet_name="2024年", usecols=[0, 1])
    df.columns = ["display_dt", "value"]
    df["display_dt"] = pd.to_datetime(df["display_dt"], errors="coerce")

    assert df.loc[0, "display_dt"] == pd.Timestamp("2024-12-01 01:00")
    assert df.loc[df.index.max(), "display_dt"] == pd.Timestamp("2025-01-01 00:00")


def test_process_period_date_display_for_code_excel_smoke(fake_bs4, fake_station_payload, make_values_payload, fake_throttled_get_factory, monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    values = ["1", "2", "3", "4", "5"]
    payloads = [fake_station_payload, make_values_payload(values)]
    monkeypatch.setattr(entry, "throttled_get", fake_throttled_get_factory(payloads))

    file_path = entry.process_period_date_display_for_code(
        code="999",
        Y1="2024",
        Y2="2024",
        M1="1月",
        M2="1月",
        mode_type="S",
        single_sheet=True,
    )

    xls = pd.ExcelFile(file_path)
    assert "2024年" in xls.sheet_names
    assert "全期間" in xls.sheet_names


def test_export_water_info_parquet_hourly_uses_datetime_for_observed_at(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    df = pd.DataFrame(
        {
            "datetime": pd.date_range("2025-01-01 00:00:00", periods=3, freq="h"),
            "display_dt": pd.date_range("2025-01-01 01:00:00", periods=3, freq="h"),
            "雨量": [1.0, 2.0, 3.0],
        }
    )

    path = entry._export_water_info_parquet(
        df=df,
        code="2700000001",
        station_name="dummy",
        mode_type="U",
        interval="1hour",
        year_start="2025",
        month_start="1月",
        year_end="2025",
        month_end="1月",
        value_col="雨量",
    )

    saved = pd.read_parquet(path, engine="pyarrow")
    observed = pd.to_datetime(saved["observed_at"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S").tolist()
    assert observed == [
        "2025-01-01 00:00:00",
        "2025-01-01 01:00:00",
        "2025-01-01 02:00:00",
    ]


def test_export_water_info_parquet_hourly_fallback_display_dt_minus_1h(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    df = pd.DataFrame(
        {
            "display_dt": pd.date_range("2025-01-01 01:00:00", periods=2, freq="h"),
            "雨量": [1.0, 2.0],
        }
    )

    path = entry._export_water_info_parquet(
        df=df,
        code="2700000002",
        station_name="dummy",
        mode_type="U",
        interval="1hour",
        year_start="2025",
        month_start="1月",
        year_end="2025",
        month_end="1月",
        value_col="雨量",
    )

    saved = pd.read_parquet(path, engine="pyarrow")
    observed = pd.to_datetime(saved["observed_at"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S").tolist()
    assert observed == [
        "2025-01-01 00:00:00",
        "2025-01-01 01:00:00",
    ]
