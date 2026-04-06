from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from river_meta.rainfall.outputs.analysis import build_annual_max_dataframe, build_hourly_timeseries_dataframe
from river_meta.rainfall.outputs.excel_exporter import export_station_rainfall_excel
from river_meta.rainfall.sources.jma.availability import JmaAvailabilityResult
from river_meta.rainfall.domain.models import JMAStationInput, RainfallRecord
from river_meta.rainfall.services import RainfallRunInput, run_rainfall_analyze, run_rainfall_collect


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
    assert "/" not in str(summary.loc[0, "1時間欠測数"])


def test_run_rainfall_analyze_rejects_non_hourly_interval():
    config = RainfallRunInput(
        source="jma",
        start_at=datetime(2025, 1, 1, 0, 0, 0),
        end_at=datetime(2025, 1, 31, 23, 59, 59),
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


def test_summary_creates_one_row_per_year_for_same_station(tmp_path):
    source_df = pd.concat(
        [
            _raw_records(
                [
                    ("2024-01-01 00:00:00", 1.0),
                    ("2024-01-01 01:00:00", 2.0),
                    ("2024-01-01 02:00:00", 3.0),
                ]
            ),
            _raw_records(
                [
                    ("2025-01-01 00:00:00", 4.0),
                    ("2025-01-01 01:00:00", 5.0),
                    ("2025-01-01 02:00:00", 6.0),
                ]
            ),
        ],
        ignore_index=True,
    )
    ts = build_hourly_timeseries_dataframe(source_df)
    annual = build_annual_max_dataframe(ts)

    path = export_station_rainfall_excel(
        ts,
        annual,
        output_path=str(tmp_path / "multi_year.xlsx"),
        decimal_places=2,
    )
    assert path is not None

    summary = pd.read_excel(path, sheet_name="年別サマリ")
    assert len(summary) == 2
    assert summary["西暦"].tolist() == [2024, 2025]


def test_run_rainfall_collect_waterinfo_prefecture_resolve_path(monkeypatch):
    captured: dict[str, object] = {}

    def _fake_resolve(prefectures, log=None):
        assert prefectures == ["大阪", "京都"]
        return ["2700000001", "2600000001"], []

    def _fake_fetch(*, stations, query, include_raw, log_warn, should_stop):
        captured["codes"] = [station.station_code for station in stations]
        captured["interval"] = query.interval
        return []

    monkeypatch.setattr("river_meta.rainfall.sources.station_resolution.resolve_waterinfo_station_codes_from_prefectures", _fake_resolve)
    monkeypatch.setattr("river_meta.rainfall.sources.fetch_water_info.fetch_waterinfo_rainfall", _fake_fetch)

    config = RainfallRunInput(
        source="water_info",
        start_at=datetime(2025, 1, 1, 0, 0, 0),
        end_at=datetime(2025, 1, 1, 23, 0, 0),
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

    monkeypatch.setattr("river_meta.rainfall.sources.fetch_water_info.fetch_waterinfo_rainfall", _fake_fetch)

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

    monkeypatch.setattr("river_meta.rainfall.sources.fetch_water_info.fetch_waterinfo_rainfall", _fake_fetch)

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

    monkeypatch.setattr("river_meta.rainfall.sources.station_resolution.resolve_jma_stations_from_codes", _fake_resolve_jma)
    monkeypatch.setattr("river_meta.rainfall.sources.fetch_jma.fetch_jma_rainfall", _fake_fetch_jma)
    monkeypatch.setattr("river_meta.rainfall.sources.fetch_water_info.fetch_waterinfo_rainfall", _fake_fetch_waterinfo)

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

    monkeypatch.setattr("river_meta.rainfall.sources.station_resolution.resolve_jma_stations_from_codes", _fake_resolve_jma)
    monkeypatch.setattr("river_meta.rainfall.sources.fetch_jma.fetch_jma_rainfall", _fake_fetch_jma)
    monkeypatch.setattr("river_meta.rainfall.sources.fetch_water_info.fetch_waterinfo_rainfall", _fake_fetch_waterinfo)

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

    monkeypatch.setattr("river_meta.rainfall.sources.station_resolution.resolve_jma_stations_from_codes", _fake_resolve_jma)
    monkeypatch.setattr("river_meta.rainfall.sources.fetch_jma.fetch_jma_rainfall", _fake_fetch_jma)

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


def test_run_rainfall_analyze_applies_jma_year_filter_and_fallback(monkeypatch, tmp_path):
    called_years: list[tuple[str, int]] = []
    logs: list[str] = []

    def _fake_fetch_available_years_hourly(*, prec_no, block_no, timeout_sec=10.0, user_agent="river-meta/0.1"):  # noqa: ARG001
        if block_no == "62001":
            return JmaAvailabilityResult(
                status="success_with_years",
                years={2025},
                reason="matched_index_links",
            )
        return JmaAvailabilityResult(
            status="indeterminate",
            years=set(),
            reason="context_mismatch",
        )

    def _fake_fetch_jma_year_monthly(
        *,
        station_obj_list,
        station_key,
        year,
        output_dir,
        config,
        logger,
        should_stop,
        all_errors,
        records_counter,
        created_parquet_paths,
    ):  # noqa: ARG001
        called_years.append((station_key, year))
        return pd.DataFrame()

    monkeypatch.setattr(
        "river_meta.rainfall.services.analyze.fetch_available_years_hourly",
        _fake_fetch_available_years_hourly,
    )
    monkeypatch.setattr(
        "river_meta.rainfall.services.analyze._fetch_jma_year_monthly",
        _fake_fetch_jma_year_monthly,
    )

    config = RainfallRunInput(
        source="jma",
        years=[2024, 2025],
        interval="1hour",
        jma_stations=[("11", "62001", "s1"), ("12", "62002", "s1")],
    )
    run_rainfall_analyze(
        config,
        output_dir=str(tmp_path),
        log=logs.append,
    )

    assert called_years == [
        ("11_62001", 2025),
        ("12_62002", 2024),
        ("12_62002", 2025),
    ]
    assert any("[collect][period][global]" in line for line in logs)
    assert any("[collect][period][station] source=jma 観測所=11_62001" in line for line in logs)
    assert any("観測所=11_62001" in line and "対象年=2025" in line for line in logs)
    assert any("観測所=11_62001 指定年数=2 -> 判定後年数=1 status=success_with_years" in line for line in logs)
    assert any("status=indeterminate" in line and "従来モードで継続" in line for line in logs)
    assert any("全体年判定: 4 -> 3 (1 年削減)" in line for line in logs)


def test_run_rainfall_analyze_collection_order_station_year(monkeypatch, tmp_path):
    called_jobs: list[tuple[str, str, int]] = []

    def _fake_fetch_available_years_hourly(*, prec_no, block_no, timeout_sec=10.0, user_agent="river-meta/0.1"):  # noqa: ARG001
        return JmaAvailabilityResult(
            status="indeterminate",
            years=set(),
            reason="context_mismatch",
        )

    def _fake_fetch_jma_year_monthly(
        *,
        station_obj_list,
        station_key,
        year,
        output_dir,
        config,
        logger,
        should_stop,
        all_errors,
        records_counter,
        created_parquet_paths,
    ):  # noqa: ARG001
        called_jobs.append(("jma", station_key, year))
        return pd.DataFrame()

    monkeypatch.setattr(
        "river_meta.rainfall.services.analyze.fetch_available_years_hourly",
        _fake_fetch_available_years_hourly,
    )
    monkeypatch.setattr(
        "river_meta.rainfall.services.analyze._fetch_jma_year_monthly",
        _fake_fetch_jma_year_monthly,
    )

    config = RainfallRunInput(
        source="jma",
        years=[2025, 2024],
        interval="1hour",
        jma_stations=[("12", "62002", "s1"), ("11", "62001", "s1")],
        collection_order="station_year",
    )
    run_rainfall_analyze(config, output_dir=str(tmp_path))

    assert called_jobs == [
        ("jma", "11_62001", 2024),
        ("jma", "11_62001", 2025),
        ("jma", "12_62002", 2024),
        ("jma", "12_62002", 2025),
    ]


def test_run_rainfall_analyze_groups_jma_obs_types_by_station_key(monkeypatch, tmp_path):
    called_jobs: list[tuple[str, int, list[str]]] = []

    def _fake_fetch_available_years_hourly(*, prec_no, block_no, timeout_sec=10.0, user_agent="river-meta/0.1"):  # noqa: ARG001
        return JmaAvailabilityResult(
            status="indeterminate",
            years=set(),
            reason="context_mismatch",
        )

    def _fake_fetch_jma_year_monthly(
        *,
        station_obj_list,
        station_key,
        year,
        output_dir,
        config,
        logger,
        should_stop,
        all_errors,
        records_counter,
        created_parquet_paths,
    ):  # noqa: ARG001
        called_jobs.append((station_key, year, [station.obs_type for station in station_obj_list]))
        return pd.DataFrame()

    monkeypatch.setattr(
        "river_meta.rainfall.services.analyze.fetch_available_years_hourly",
        _fake_fetch_available_years_hourly,
    )
    monkeypatch.setattr(
        "river_meta.rainfall.services.analyze._fetch_jma_year_monthly",
        _fake_fetch_jma_year_monthly,
    )

    config = RainfallRunInput(
        source="jma",
        years=[2025],
        interval="1hour",
        jma_stations=[("11", "62001", "s1"), ("11", "62001", "a1")],
    )
    run_rainfall_analyze(config, output_dir=str(tmp_path))

    assert called_jobs == [
        ("11_62001", 2025, ["s1", "a1"]),
    ]


def test_run_rainfall_analyze_collection_order_year_station(monkeypatch, tmp_path):
    called_jobs: list[tuple[str, str, int]] = []

    def _fake_fetch_available_years_hourly(*, prec_no, block_no, timeout_sec=10.0, user_agent="river-meta/0.1"):  # noqa: ARG001
        return JmaAvailabilityResult(
            status="indeterminate",
            years=set(),
            reason="context_mismatch",
        )

    def _fake_fetch_jma_year_monthly(
        *,
        station_obj_list,
        station_key,
        year,
        output_dir,
        config,
        logger,
        should_stop,
        all_errors,
        records_counter,
        created_parquet_paths,
    ):  # noqa: ARG001
        called_jobs.append(("jma", station_key, year))
        return pd.DataFrame()

    monkeypatch.setattr(
        "river_meta.rainfall.services.analyze.fetch_available_years_hourly",
        _fake_fetch_available_years_hourly,
    )
    monkeypatch.setattr(
        "river_meta.rainfall.services.analyze._fetch_jma_year_monthly",
        _fake_fetch_jma_year_monthly,
    )

    config = RainfallRunInput(
        source="jma",
        years=[2025, 2024],
        interval="1hour",
        jma_stations=[("12", "62002", "s1"), ("11", "62001", "s1")],
        collection_order=" Year-Station ",
    )
    run_rainfall_analyze(config, output_dir=str(tmp_path))

    assert called_jobs == [
        ("jma", "11_62001", 2024),
        ("jma", "12_62002", 2024),
        ("jma", "11_62001", 2025),
        ("jma", "12_62002", 2025),
    ]


def test_run_rainfall_analyze_collection_order_source_both_is_stable(monkeypatch, tmp_path):
    called_jobs: list[tuple[str, str, int]] = []

    def _fake_fetch_available_years_hourly(*, prec_no, block_no, timeout_sec=10.0, user_agent="river-meta/0.1"):  # noqa: ARG001
        return JmaAvailabilityResult(
            status="indeterminate",
            years=set(),
            reason="context_mismatch",
        )

    def _fake_fetch_jma_year_monthly(
        *,
        station_obj_list,
        station_key,
        year,
        output_dir,
        config,
        logger,
        should_stop,
        all_errors,
        records_counter,
        created_parquet_paths,
    ):  # noqa: ARG001
        called_jobs.append(("jma", station_key, year))
        return pd.DataFrame()

    def _fake_fetch_waterinfo_year(
        *,
        station_obj_list,
        station_key,
        year,
        output_dir,
        config,
        logger,
        should_stop,
        all_errors,
        created_parquet_paths,
    ):  # noqa: ARG001
        called_jobs.append(("water_info", station_key, year))
        return pd.DataFrame()

    monkeypatch.setattr(
        "river_meta.rainfall.services.analyze.fetch_available_years_hourly",
        _fake_fetch_available_years_hourly,
    )
    monkeypatch.setattr(
        "river_meta.rainfall.services.analyze._fetch_jma_year_monthly",
        _fake_fetch_jma_year_monthly,
    )
    monkeypatch.setattr(
        "river_meta.rainfall.services.analyze._fetch_waterinfo_year",
        _fake_fetch_waterinfo_year,
    )

    config = RainfallRunInput(
        source="both",
        years=[2025, 2024],
        interval="1hour",
        jma_stations=[("11", "62001", "s1")],
        waterinfo_station_codes=["2700000001"],
        collection_order="year_station",
    )
    run_rainfall_analyze(config, output_dir=str(tmp_path))

    assert called_jobs == [
        ("jma", "11_62001", 2024),
        ("water_info", "2700000001", 2024),
        ("jma", "11_62001", 2025),
        ("water_info", "2700000001", 2025),
    ]


def test_run_rainfall_analyze_resolves_years_from_datetime_range(monkeypatch, tmp_path):
    called_jobs: list[tuple[str, int]] = []
    logs: list[str] = []

    def _fake_fetch_waterinfo_year(
        *,
        station_obj_list,
        station_key,
        year,
        output_dir,
        config,
        logger,
        should_stop,
        all_errors,
        created_parquet_paths,
    ):  # noqa: ARG001
        called_jobs.append((station_key, year))
        return pd.DataFrame()

    monkeypatch.setattr(
        "river_meta.rainfall.services.analyze._fetch_waterinfo_year",
        _fake_fetch_waterinfo_year,
    )

    config = RainfallRunInput(
        source="water_info",
        start_at=datetime(2024, 6, 1, 0, 0, 0),
        end_at=datetime(2026, 2, 1, 23, 59, 59),
        interval="1hour",
        waterinfo_station_codes=["2700000001"],
    )
    run_rainfall_analyze(config, output_dir=str(tmp_path), log=logs.append)

    assert called_jobs == [
        ("2700000001", 2024),
        ("2700000001", 2025),
        ("2700000001", 2026),
    ]
    assert any("[collect][period][global]" in line and "正規化後対象年=2024, 2025, 2026" in line for line in logs)
    assert any("[collect][period][station] source=water_info 観測所=2700000001" in line for line in logs)
    assert any("取得期間=2024-01-01 00:00:00 ～ 2026-12-31 23:59:59" in line for line in logs)
