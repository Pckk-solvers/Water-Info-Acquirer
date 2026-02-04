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
