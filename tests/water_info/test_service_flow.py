import pandas as pd

from src.water_info.service import flow_fetch, flow_write


def test_fetch_hourly_dataframe_for_code_builds_df_and_path(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    def _station(*args, **kwargs):
        return "テスト観測所"

    def _values(*args, **kwargs):
        return [1.0] * (24 * 2)

    monkeypatch.setattr(flow_fetch, "fetch_station_name", _station)
    monkeypatch.setattr(flow_fetch, "fetch_hourly_values", _values)

    df, file_name, value_col = flow_fetch.fetch_hourly_dataframe_for_code(
        code="123",
        year_start="2024",
        year_end="2024",
        month_start="1月",
        month_end="1月",
        mode_type="S",
        throttled_get=lambda *a, **k: None,
        headers={},
    )

    assert value_col == "水位"
    assert df is not None
    assert len(df) == 48
    assert "テスト観測所" in str(file_name)


def test_write_hourly_excel_creates_file(tmp_path):
    df = pd.DataFrame(
        {
            "display_dt": pd.date_range("2024-01-01", periods=3, freq="h"),
            "水位": [1.0, 2.0, 3.0],
            "sheet_year": [2024, 2024, 2024],
        }
    )
    file_path = tmp_path / "hourly.xlsx"
    flow_write.write_hourly_excel(
        df=df,
        file_name=file_path,
        value_col="水位",
        mode_type="S",
        single_sheet=True,
    )
    assert file_path.exists()


def test_fetch_daily_dataframe_for_code_builds_df_and_path(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)

    def _station(*args, **kwargs):
        return "テスト観測所"

    def _values(*args, **kwargs):
        return [1.0] * 31

    monkeypatch.setattr(flow_fetch, "fetch_station_name", _station)
    monkeypatch.setattr(flow_fetch, "fetch_daily_values", _values)

    df, file_name, data_label, chart_title = flow_fetch.fetch_daily_dataframe_for_code(
        code="456",
        year_start="2024",
        year_end="2024",
        month_start="1月",
        month_end="1月",
        mode_type="S",
        throttled_get=lambda *a, **k: None,
        headers={},
    )

    assert data_label == "水位"
    assert chart_title == "水位[m]"
    assert df is not None
    assert "テスト観測所" in str(file_name)


def test_write_daily_excel_creates_file(tmp_path):
    df = pd.DataFrame({"水位": [1.0, 2.0, 3.0]}, index=pd.date_range("2024-01-01", periods=3, freq="D"))
    file_path = tmp_path / "daily.xlsx"
    flow_write.write_daily_excel(
        df=df,
        file_name=file_path,
        data_label="水位",
        chart_title="水位[m]",
        single_sheet=True,
    )
    assert file_path.exists()
