from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from river_meta.rainfall.analysis import build_annual_max_dataframe, build_hourly_timeseries_dataframe
from river_meta.rainfall.excel_exporter import export_station_rainfall_excel
from river_meta.rainfall.models import JMAStationInput, RainfallRecord
from river_meta.services.rainfall import RainfallRunInput, run_rainfall_analyze, run_rainfall_collect


def _raw_records(values: list[tuple[str, float | None]]) -> pd.DataFrame:
    rows = []
    for ts, rainfall in values:
        rows.append(
            {
                "source": "jma",
                "station_key": "47401",
                "station_name": "さいたま",
                "observed_at": ts,
                "interval": "1hour",
                "rainfall_mm": rainfall,
                "quality": "normal" if rainfall is not None else "missing",
            }
        )
    return pd.DataFrame(rows)


def test_build_hourly_timeseries_rollings_and_head_nan():
    source_df = _raw_records(
        [
            ("2025-01-01 00:00:00", 1.0),
            ("2025-01-01 01:00:00", 2.0),
            ("2025-01-01 02:00:00", 3.0),
            ("2025-01-01 03:00:00", 4.0),
        ]
    )

    ts = build_hourly_timeseries_dataframe(source_df)
    assert len(ts) == 4
    assert pd.isna(ts.loc[0, "3時間雨量(mm)"])
    assert pd.isna(ts.loc[1, "3時間雨量(mm)"])
    assert ts.loc[2, "3時間雨量(mm)"] == 6.0
    assert ts.loc[3, "3時間雨量(mm)"] == 9.0
    assert pd.isna(ts.loc[3, "6時間雨量(mm)"])


def test_build_hourly_timeseries_gap_causes_nan_window():
    source_df = _raw_records(
        [
            ("2025-01-01 00:00:00", 1.0),
            ("2025-01-01 01:00:00", 2.0),
            ("2025-01-01 03:00:00", 4.0),
        ]
    )

    ts = build_hourly_timeseries_dataframe(source_df)
    assert len(ts) == 4
    # 02:00 が欠けているので再インデックスでNaNが挿入される
    assert pd.isna(ts.loc[2, "1時間雨量(mm)"])
    # 03:00 時点の3時間窓 (01:00-03:00) に NaN を含むため NaN
    assert pd.isna(ts.loc[3, "3時間雨量(mm)"])


def test_build_annual_max_tie_uses_earliest_and_marks_reference():
    source_df = _raw_records(
        [
            ("2025-01-01 00:00:00", 1.0),
            ("2025-01-01 01:00:00", 5.0),
            ("2025-01-01 02:00:00", 5.0),
            ("2025-01-01 03:00:00", 2.0),
        ]
    )

    ts = build_hourly_timeseries_dataframe(source_df)
    annual = build_annual_max_dataframe(ts)

    row = annual[(annual["年"] == 2025) & (annual["指標"] == "1時間雨量")].iloc[0]
    assert row["最大雨量(mm)"] == 5.0
    assert row["発生日時"] == pd.Timestamp("2025-01-01 01:00:00")
    assert bool(row["年間完全性"]) is False
    assert "参考値" in row["備考"]


def test_export_consolidated_rainfall_excel_creates_single_file(tmp_path):
    source_df = _raw_records(
        [
            ("2025-01-01 00:00:00", 1.234),
            ("2025-01-01 01:00:00", 2.345),
            ("2025-01-01 02:00:00", 3.456),
        ]
    )
    ts = build_hourly_timeseries_dataframe(source_df)
    annual = build_annual_max_dataframe(ts)

    output = tmp_path / "rain_summary.xlsx"
    path = export_station_rainfall_excel(ts, annual, output_path=str(output), decimal_places=2)
    assert path is not None
    assert path.exists()

    xls = pd.ExcelFile(path)
    assert "年別サマリ" in xls.sheet_names
    assert "時系列データ" in xls.sheet_names
    assert "年最大雨量一覧" in xls.sheet_names

    summary = pd.read_excel(path, sheet_name="年別サマリ")
    assert len(summary) == 1
    assert "西暦" in summary.columns
    assert "和暦" in summary.columns
    assert "集計開始" not in summary.columns
    assert "集計終了" not in summary.columns
    assert "年間完全性" not in summary.columns
    assert "備考" not in summary.columns


def test_run_rainfall_analyze_rejects_non_hourly_interval():
    config = RainfallRunInput(
        source="jma",
        start_at=pd.Timestamp("2025-01-01").to_pydatetime(),
        end_at=pd.Timestamp("2025-01-31 23:59:59").to_pydatetime(),
        interval="1day",
        jma_station_codes=["47401"],
    )
    result = run_rainfall_analyze(config)
    assert result.dataset.errors
    assert "1hour" in result.dataset.errors[0]


def test_export_consolidated_summary_is_one_row_per_station(tmp_path):
    s1 = _raw_records(
        [
            ("2025-01-01 00:00:00", 1.0),
            ("2025-01-01 01:00:00", 2.0),
        ]
    )
    s2 = _raw_records(
        [
            ("2025-01-01 00:00:00", 3.0),
            ("2025-01-01 01:00:00", 4.0),
        ]
    )
    s2["station_key"] = "47402"
    s2["station_name"] = "旭川"
    source_df = pd.concat([s1, s2], ignore_index=True)
    ts = build_hourly_timeseries_dataframe(source_df)
    annual = build_annual_max_dataframe(ts)

    path = export_station_rainfall_excel(
        ts,
        annual,
        output_path=str(tmp_path / "stations.xlsx"),
        decimal_places=2,
    )
    assert path is not None
    summary = pd.read_excel(path, sheet_name="年別サマリ")
    assert len(summary) == 2


def test_summary_has_wareki_value_for_single_year(tmp_path):
    source_df = _raw_records(
        [
            ("2011-01-01 00:00:00", 1.0),
            ("2011-01-01 01:00:00", 2.0),
            ("2011-01-01 02:00:00", 3.0),
        ]
    )
    ts = build_hourly_timeseries_dataframe(source_df)
    annual = build_annual_max_dataframe(ts)
    path = export_station_rainfall_excel(
        ts,
        annual,
        output_path=str(tmp_path / "wareki.xlsx"),
        decimal_places=2,
    )
    assert path is not None
    summary = pd.read_excel(path, sheet_name="年別サマリ")
    assert int(summary.loc[0, "西暦"]) == 2011
    assert summary.loc[0, "和暦"] == "H23"


def test_run_rainfall_collect_waterinfo_prefecture_resolve_path(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_resolve(prefectures, log=None):
        assert prefectures == ["大阪", "京都"]
        return ["2700000001", "2600000001"], []

    def _fake_fetch(*, stations, query, include_raw, log_warn, should_stop):
        captured["codes"] = [station.station_code for station in stations]
        captured["interval"] = query.interval
        return []

    monkeypatch.setattr("river_meta.services.rainfall.resolve_waterinfo_station_codes_from_prefectures", _fake_resolve)
    monkeypatch.setattr("river_meta.services.rainfall.fetch_waterinfo_rainfall", _fake_fetch)

    config = RainfallRunInput(
        source="water_info",
        start_at=pd.Timestamp("2025-01-01 00:00:00").to_pydatetime(),
        end_at=pd.Timestamp("2025-01-01 23:00:00").to_pydatetime(),
        interval="1hour",
        waterinfo_prefectures=["大阪", "京都"],
        waterinfo_station_codes=["2700000001"],
    )
    result = run_rainfall_collect(config)
    assert result.errors == []
    assert captured["codes"] == ["2600000001", "2700000001"]
    assert captured["interval"] == "1hour"


def test_run_rainfall_collect_with_year_expands_to_calendar_year(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_fetch(*, stations, query, include_raw, log_warn, should_stop):
        captured["start_at"] = query.start_at
        captured["end_at"] = query.end_at
        return []

    monkeypatch.setattr("river_meta.services.rainfall.fetch_waterinfo_rainfall", _fake_fetch)

    config = RainfallRunInput(
        source="water_info",
        year=2025,
        interval="1hour",
        waterinfo_station_codes=["2700000001"],
    )
    result = run_rainfall_collect(config)
    assert result.errors == []
    assert captured["start_at"] == datetime(2025, 1, 1, 0, 0, 0)
    assert captured["end_at"] == datetime(2025, 12, 31, 23, 59, 59)


def test_run_rainfall_collect_cancelled_before_fetch(monkeypatch):
    called = {"fetch": False}

    def _fake_fetch(*, stations, query, include_raw, log_warn, should_stop):
        called["fetch"] = True
        return []

    monkeypatch.setattr("river_meta.services.rainfall.fetch_waterinfo_rainfall", _fake_fetch)

    config = RainfallRunInput(
        source="water_info",
        year=2025,
        interval="1hour",
        waterinfo_station_codes=["2700000001"],
    )
    result = run_rainfall_collect(config, should_stop=lambda: True)
    assert result.errors == ["cancelled"]
    assert called["fetch"] is False


def test_run_rainfall_collect_rejects_year_and_range_together():
    config = RainfallRunInput(
        source="water_info",
        start_at=datetime(2025, 1, 1, 0, 0, 0),
        end_at=datetime(2025, 1, 31, 23, 0, 0),
        year=2025,
        interval="1hour",
        waterinfo_station_codes=["2700000001"],
    )
    result = run_rainfall_collect(config)
    assert result.errors
    assert "either year/years or start_at/end_at" in result.errors[0]


def test_run_rainfall_collect_source_both_merges_records(monkeypatch):
    def _fake_resolve_jma(station_codes, *, index_data=None, index_path=None):
        return [JMAStationInput(prefecture_code="27", block_number="62001", obs_type="s1", station_name="大阪")], []

    def _fake_fetch_jma(*, stations, query, include_raw, log_warn, should_stop, jma_log_level, jma_enable_log_output):
        return [
            RainfallRecord(
                source="jma",
                station_key="62001",
                station_name="大阪",
                observed_at=datetime(2025, 1, 1, 0, 0, 0),
                interval="1hour",
                rainfall_mm=1.0,
            )
        ]

    def _fake_fetch_waterinfo(*, stations, query, include_raw, log_warn, should_stop):
        return [
            RainfallRecord(
                source="water_info",
                station_key="2700000001",
                station_name="大阪",
                observed_at=datetime(2025, 1, 1, 0, 0, 0),
                interval="1hour",
                rainfall_mm=2.0,
            )
        ]

    monkeypatch.setattr("river_meta.services.rainfall.resolve_jma_stations_from_codes", _fake_resolve_jma)
    monkeypatch.setattr("river_meta.services.rainfall.fetch_jma_rainfall", _fake_fetch_jma)
    monkeypatch.setattr("river_meta.services.rainfall.fetch_waterinfo_rainfall", _fake_fetch_waterinfo)

    config = RainfallRunInput(
        source="both",
        year=2025,
        interval="1hour",
        jma_station_codes=["62001"],
        waterinfo_station_codes=["2700000001"],
    )
    result = run_rainfall_collect(config)
    assert result.errors == []
    assert len(result.records) == 2
    assert {record.source for record in result.records} == {"jma", "water_info"}


def test_run_rainfall_analyze_source_both_continues_when_one_source_fails(monkeypatch, tmp_path):
    def _fake_resolve_jma(station_codes, *, index_data=None, index_path=None):
        return [JMAStationInput(prefecture_code="27", block_number="62001", obs_type="s1", station_name="大阪")], []

    def _fake_fetch_jma(*, stations, query, include_raw, log_warn, should_stop, jma_log_level, jma_enable_log_output):
        raise RuntimeError("boom")

    def _fake_fetch_waterinfo(*, stations, query, include_raw, log_warn, should_stop):
        return [
            RainfallRecord(
                source="water_info",
                station_key="2700000001",
                station_name="大阪",
                observed_at=datetime(2025, 1, 1, 0, 0, 0),
                interval="1hour",
                rainfall_mm=2.0,
            )
        ]

    monkeypatch.setattr("river_meta.services.rainfall.resolve_jma_stations_from_codes", _fake_resolve_jma)
    monkeypatch.setattr("river_meta.services.rainfall.fetch_jma_rainfall", _fake_fetch_jma)
    monkeypatch.setattr("river_meta.services.rainfall.fetch_waterinfo_rainfall", _fake_fetch_waterinfo)

    config = RainfallRunInput(
        source="both",
        year=2025,
        interval="1hour",
        jma_station_codes=["62001"],
        waterinfo_station_codes=["2700000001"],
    )
    result = run_rainfall_analyze(config, output_dir=str(tmp_path))
    assert result.dataset.errors
    assert "jma:RuntimeError" in result.dataset.errors[0]


def test_run_rainfall_collect_passes_jma_log_controls(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_resolve_jma(station_codes, *, index_data=None, index_path=None):
        return [JMAStationInput(prefecture_code="27", block_number="62001", obs_type="s1", station_name="大阪")], []

    def _fake_fetch_jma(*, stations, query, include_raw, log_warn, should_stop, jma_log_level, jma_enable_log_output):
        captured["jma_log_level"] = jma_log_level
        captured["jma_enable_log_output"] = jma_enable_log_output
        return []

    monkeypatch.setattr("river_meta.services.rainfall.resolve_jma_stations_from_codes", _fake_resolve_jma)
    monkeypatch.setattr("river_meta.services.rainfall.fetch_jma_rainfall", _fake_fetch_jma)

    config = RainfallRunInput(
        source="jma",
        year=2025,
        interval="1hour",
        jma_station_codes=["62001"],
        jma_log_level="DEBUG",
        jma_enable_log_output=False,
    )
    result = run_rainfall_collect(config)
    assert result.errors == []
    assert captured["jma_log_level"] == "DEBUG"
    assert captured["jma_enable_log_output"] is False
