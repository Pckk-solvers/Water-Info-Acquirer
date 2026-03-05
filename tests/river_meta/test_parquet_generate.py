"""parquet_store スキャン機能 + run_rainfall_generate のテスト。"""
from __future__ import annotations

import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))
from river_meta.rainfall.analysis import build_annual_max_dataframe, build_hourly_timeseries_dataframe
from river_meta.rainfall.parquet_store import (
    ParquetEntry,
    build_parquet_path,
    find_missing_months,
    save_records_parquet,
    scan_parquet_dir,
)
from river_meta.rainfall.models import RainfallRecord
from river_meta.services.rainfall import (
    RainfallGenerateInput,
    run_rainfall_generate,
)


# ---------------------------------------------------------------------------
# ヘルパー
# ---------------------------------------------------------------------------

def _make_dummy_records(
    source: str,
    station_key: str,
    year: int,
    month: int,
    *,
    station_name: str = "テスト観測所",
) -> list[RainfallRecord]:
    """指定月のダミー RainfallRecord リストを生成。"""
    import calendar
    last_day = calendar.monthrange(year, month)[1]
    records = []
    for day in range(1, last_day + 1):
        for hour in range(24):
            records.append(RainfallRecord(
                source=source,
                station_key=station_key,
                station_name=station_name,
                observed_at=f"{year}-{month:02d}-{day:02d} {hour:02d}:00:00",
                interval="1hour",
                rainfall_mm=round(np.random.exponential(1.0), 1),
                quality="normal",
            ))
    return records


def _create_full_year_parquets(
    output_dir: Path,
    source: str,
    station_key: str,
    year: int,
    *,
    months: list[int] | None = None,
) -> None:
    """指定した月のParquetファイルを作成する。"""
    target_months = months or list(range(1, 13))
    if source == "water_info":
        # water_info は年単位
        all_records: list[RainfallRecord] = []
        for m in target_months:
            all_records.extend(_make_dummy_records(source, station_key, year, m))
        pq_path = build_parquet_path(output_dir, source, station_key, year)
        save_records_parquet(all_records, pq_path)
    else:
        # JMA は月単位
        for m in target_months:
            records = _make_dummy_records(source, station_key, year, m)
            pq_path = build_parquet_path(output_dir, source, station_key, year, month=m)
            save_records_parquet(records, pq_path)


# ---------------------------------------------------------------------------
# scan_parquet_dir テスト
# ---------------------------------------------------------------------------

def test_scan_empty_dir(tmp_path):
    assert scan_parquet_dir(str(tmp_path)) == []


def test_scan_jma_complete_year(tmp_path):
    _create_full_year_parquets(tmp_path, "jma", "47401", 2024)
    entries = scan_parquet_dir(str(tmp_path))
    assert len(entries) == 1
    assert entries[0].source == "jma"
    assert entries[0].station_key == "47401"
    assert entries[0].year == 2024
    assert entries[0].complete is True
    assert entries[0].months == list(range(1, 13))


def test_scan_jma_incomplete_year(tmp_path):
    _create_full_year_parquets(tmp_path, "jma", "47401", 2024, months=[1, 2, 3])
    entries = scan_parquet_dir(str(tmp_path))
    assert len(entries) == 1
    assert entries[0].complete is False
    assert entries[0].months == [1, 2, 3]


def test_scan_waterinfo_year(tmp_path):
    _create_full_year_parquets(tmp_path, "water_info", "1361160200060", 2024)
    entries = scan_parquet_dir(str(tmp_path))
    assert len(entries) == 1
    assert entries[0].source == "water_info"
    assert entries[0].complete is True


def test_scan_multiple_stations_years(tmp_path):
    _create_full_year_parquets(tmp_path, "jma", "47401", 2023)
    _create_full_year_parquets(tmp_path, "jma", "47401", 2024)
    _create_full_year_parquets(tmp_path, "jma", "47772", 2024, months=[1, 2])
    entries = scan_parquet_dir(str(tmp_path))
    assert len(entries) == 3


# ---------------------------------------------------------------------------
# find_missing_months テスト
# ---------------------------------------------------------------------------

def test_find_missing_months_complete(tmp_path):
    _create_full_year_parquets(tmp_path, "jma", "47401", 2024)
    missing = find_missing_months(str(tmp_path), "jma", "47401", 2024)
    assert missing == []


def test_find_missing_months_partial(tmp_path):
    _create_full_year_parquets(tmp_path, "jma", "47401", 2024, months=[1, 2, 3, 10, 11, 12])
    missing = find_missing_months(str(tmp_path), "jma", "47401", 2024)
    assert missing == [4, 5, 6, 7, 8, 9]


def test_find_missing_months_waterinfo_exists(tmp_path):
    _create_full_year_parquets(tmp_path, "water_info", "CODE1", 2024)
    missing = find_missing_months(str(tmp_path), "water_info", "CODE1", 2024)
    assert missing == []


def test_find_missing_months_waterinfo_missing(tmp_path):
    missing = find_missing_months(str(tmp_path), "water_info", "CODE1", 2024)
    assert missing == [0]  # 年単位ファイルがない


# ---------------------------------------------------------------------------
# run_rainfall_generate テスト
# ---------------------------------------------------------------------------

def test_generate_no_parquets(tmp_path):
    config = RainfallGenerateInput(parquet_dir=str(tmp_path))
    result = run_rainfall_generate(config)
    assert "No parquet files found" in result.errors


def test_generate_skips_incomplete_years(tmp_path):
    _create_full_year_parquets(tmp_path, "jma", "47401", 2024, months=[1, 2, 3])
    config = RainfallGenerateInput(parquet_dir=str(tmp_path), export_excel=True, export_chart=False)
    logs: list[str] = []
    result = run_rainfall_generate(config, log=logs.append)
    assert len(result.incomplete_entries) == 1
    assert result.excel_paths == []
    assert any("不足月" in log for log in logs)


def test_generate_outputs_for_complete_year(tmp_path):
    _create_full_year_parquets(tmp_path, "jma", "47401", 2024)
    config = RainfallGenerateInput(
        parquet_dir=str(tmp_path),
        export_excel=True,
        export_chart=False,
    )
    result = run_rainfall_generate(config, log=lambda msg: None)
    assert len(result.incomplete_entries) == 0
    assert len(result.excel_paths) >= 1
    for path in result.excel_paths:
        assert Path(path).exists()


def test_generate_with_chart(tmp_path):
    _create_full_year_parquets(tmp_path, "jma", "47401", 2024)
    config = RainfallGenerateInput(
        parquet_dir=str(tmp_path),
        export_excel=False,
        export_chart=True,
    )
    result = run_rainfall_generate(config, log=lambda msg: None)
    assert len(result.chart_paths) >= 1
    for path in result.chart_paths:
        assert Path(path).exists()
        assert path.endswith(".png")


def test_generate_diff_mode_skips_on_second_run(tmp_path):
    _create_full_year_parquets(tmp_path, "jma", "47401", 2024)
    config = RainfallGenerateInput(
        parquet_dir=str(tmp_path),
        export_excel=True,
        export_chart=False,
    )
    first = run_rainfall_generate(config, log=lambda msg: None)
    assert len(first.excel_paths) == 1
    excel_path = Path(first.excel_paths[0])
    assert excel_path.exists()
    first_mtime_ns = excel_path.stat().st_mtime_ns

    second = run_rainfall_generate(config, log=lambda msg: None)
    assert second.excel_paths == []
    assert excel_path.stat().st_mtime_ns == first_mtime_ns


def test_generate_diff_mode_regenerates_when_parquet_updated(tmp_path):
    _create_full_year_parquets(tmp_path, "jma", "47401", 2024)
    config = RainfallGenerateInput(
        parquet_dir=str(tmp_path),
        export_excel=True,
        export_chart=False,
    )
    first = run_rainfall_generate(config, log=lambda msg: None)
    assert len(first.excel_paths) == 1
    excel_path = Path(first.excel_paths[0])
    first_mtime_ns = excel_path.stat().st_mtime_ns

    time.sleep(0.02)
    updated_records = _make_dummy_records("jma", "47401", 2024, 1)
    updated_records[0].rainfall_mm = 999.9
    jan_path = build_parquet_path(tmp_path, "jma", "47401", 2024, month=1)
    save_records_parquet(updated_records, jan_path)

    time.sleep(0.02)
    second = run_rainfall_generate(config, log=lambda msg: None)
    assert len(second.excel_paths) == 1
    assert excel_path.stat().st_mtime_ns > first_mtime_ns


def test_generate_force_full_regenerate_ignores_diff(tmp_path):
    _create_full_year_parquets(tmp_path, "jma", "47401", 2024)
    base_config = RainfallGenerateInput(
        parquet_dir=str(tmp_path),
        export_excel=True,
        export_chart=False,
    )
    first = run_rainfall_generate(base_config, log=lambda msg: None)
    assert len(first.excel_paths) == 1
    excel_path = Path(first.excel_paths[0])
    first_mtime_ns = excel_path.stat().st_mtime_ns

    time.sleep(0.02)
    force_config = RainfallGenerateInput(
        parquet_dir=str(tmp_path),
        export_excel=True,
        export_chart=False,
        force_full_regenerate=True,
    )
    second = run_rainfall_generate(force_config, log=lambda msg: None)
    assert len(second.excel_paths) == 1
    assert excel_path.stat().st_mtime_ns > first_mtime_ns
