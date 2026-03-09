from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from river_meta.rainfall.models import RainfallRecord
from river_meta.rainfall.parquet_store import build_parquet_path, save_records_parquet
from river_meta.services.rainfall import (
    RainfallParquetPeriodBatchExportInput,
    RainfallParquetPeriodExportInput,
    RainfallParquetPeriodExportTarget,
    export_period_targets_csv,
    load_period_targets_csv,
    run_rainfall_parquet_period_batch_export,
    run_rainfall_parquet_period_export,
)


def _record(
    *,
    source: str,
    station_key: str,
    station_name: str,
    observed_at: str,
    rainfall_mm: float | None,
) -> RainfallRecord:
    return RainfallRecord(
        source=source,  # type: ignore[arg-type]
        station_key=station_key,
        station_name=station_name,
        observed_at=datetime.fromisoformat(observed_at),
        interval="1hour",
        rainfall_mm=rainfall_mm,
        quality="normal" if rainfall_mm is not None else "missing",
    )


def test_period_export_jma_creates_range_rows_csv(tmp_path):
    records = [
        _record(
            source="jma",
            station_key="11_62001",
            station_name="大阪",
            observed_at="2025-01-03 00:00:00",
            rainfall_mm=1.0,
        ),
        _record(
            source="jma",
            station_key="11_62001",
            station_name="大阪",
            observed_at="2025-01-03 02:00:00",
            rainfall_mm=3.0,
        ),
        _record(
            source="jma",
            station_key="11_62001",
            station_name="大阪",
            observed_at="2025-01-04 23:00:00",
            rainfall_mm=7.0,
        ),
    ]
    pq_path = build_parquet_path(tmp_path, "jma", "11_62001", 2025, month=1)
    save_records_parquet(records, pq_path)

    result = run_rainfall_parquet_period_export(
        RainfallParquetPeriodExportInput(
            parquet_dir=str(tmp_path),
            output_dir=str(tmp_path / "csv"),
            source="jma",
            station_key="11_62001",
            station_name="大阪",
            display_station_code="62001",
            start_date="2025-01-03",
            end_date="2025-01-04",
        )
    )

    assert result.errors == []
    assert result.csv_path is not None
    df = pd.read_csv(result.csv_path)
    assert len(df) == 48
    assert df.loc[0, "date"] == "2025-01-03"
    assert df.loc[0, "hour"] == 1
    assert df.loc[0, "rainfall"] == 1.0
    assert pd.isna(df.loc[1, "rainfall"])
    assert df.loc[2, "rainfall"] == 3.0
    assert df.loc[47, "date"] == "2025-01-04"
    assert df.loc[47, "hour"] == 24
    assert df.loc[47, "rainfall"] == 7.0
    assert Path(result.csv_path).name == "jma_62001_大阪_2025-01-03_2025-01-04.csv"


def test_period_export_waterinfo_creates_range_rows_csv(tmp_path):
    records = [
        _record(
            source="water_info",
            station_key="2700000001",
            station_name="淀川",
            observed_at="2025-01-03 23:00:00",
            rainfall_mm=5.5,
        ),
        _record(
            source="water_info",
            station_key="2700000001",
            station_name="淀川",
            observed_at="2025-01-04 00:00:00",
            rainfall_mm=1.5,
        ),
    ]
    pq_path = build_parquet_path(tmp_path, "water_info", "2700000001", 2025)
    save_records_parquet(records, pq_path)

    result = run_rainfall_parquet_period_export(
        RainfallParquetPeriodExportInput(
            parquet_dir=str(tmp_path),
            output_dir=str(tmp_path / "csv"),
            source="water_info",
            station_key="2700000001",
            station_name="淀川",
            start_date="2025-01-03",
            end_date="2025-01-04",
        )
    )

    assert result.errors == []
    assert result.csv_path is not None
    df = pd.read_csv(result.csv_path)
    assert len(df) == 48
    assert df.loc[23, "hour"] == 24
    assert df.loc[23, "rainfall"] == 5.5
    assert df.loc[24, "date"] == "2025-01-04"
    assert df.loc[24, "hour"] == 1
    assert df.loc[24, "rainfall"] == 1.5


def test_period_export_outputs_blank_rows_when_range_has_no_data(tmp_path):
    records = [
        _record(
            source="water_info",
            station_key="2700000001",
            station_name="淀川",
            observed_at="2025-01-02 23:00:00",
            rainfall_mm=5.5,
        ),
    ]
    pq_path = build_parquet_path(tmp_path, "water_info", "2700000001", 2025)
    save_records_parquet(records, pq_path)

    result = run_rainfall_parquet_period_export(
        RainfallParquetPeriodExportInput(
            parquet_dir=str(tmp_path),
            output_dir=str(tmp_path / "csv"),
            source="water_info",
            station_key="2700000001",
            station_name="淀川",
            start_date="2025-01-03",
            end_date="2025-01-03",
        )
    )

    assert result.errors == []
    assert result.csv_path is not None
    assert result.row_count == 24
    df = pd.read_csv(result.csv_path)
    assert df["rainfall"].isna().all()


def test_period_export_accepts_direct_parquet_directory(tmp_path):
    records = [
        _record(
            source="jma",
            station_key="11_62001",
            station_name="大阪",
            observed_at="2025-01-03 00:00:00",
            rainfall_mm=1.0,
        ),
    ]
    pq_path = build_parquet_path(tmp_path, "jma", "11_62001", 2025, month=1)
    save_records_parquet(records, pq_path)

    result = run_rainfall_parquet_period_export(
        RainfallParquetPeriodExportInput(
            parquet_dir=str(tmp_path / "parquet"),
            output_dir=str(tmp_path / "csv"),
            source="jma",
            station_key="11_62001",
            station_name="大阪",
            display_station_code="62001",
            start_date="2025-01-03",
            end_date="2025-01-03",
        )
    )

    assert result.errors == []
    assert result.csv_path is not None
    df = pd.read_csv(result.csv_path)
    assert len(df) == 24


def test_period_batch_export_runs_for_multiple_targets(tmp_path):
    jma_records = [
        _record(
            source="jma",
            station_key="11_62001",
            station_name="大阪",
            observed_at="2025-01-03 00:00:00",
            rainfall_mm=1.0,
        ),
        _record(
            source="jma",
            station_key="11_62001",
            station_name="大阪",
            observed_at="2025-01-04 23:00:00",
            rainfall_mm=2.0,
        ),
    ]
    wi_records = [
        _record(
            source="water_info",
            station_key="2700000001",
            station_name="淀川",
            observed_at="2025-01-05 12:00:00",
            rainfall_mm=4.0,
        ),
    ]
    save_records_parquet(jma_records, build_parquet_path(tmp_path, "jma", "11_62001", 2025, month=1))
    save_records_parquet(wi_records, build_parquet_path(tmp_path, "water_info", "2700000001", 2025))

    result = run_rainfall_parquet_period_batch_export(
        RainfallParquetPeriodBatchExportInput(
            parquet_dir=str(tmp_path),
            output_dir=str(tmp_path / "csv"),
            targets=[
                RainfallParquetPeriodExportTarget(
                    source="jma",
                    station_key="11_62001",
                    station_name="大阪",
                    display_station_code="62001",
                    start_date="2025-01-03",
                    end_date="2025-01-04",
                ),
                RainfallParquetPeriodExportTarget(
                    source="water_info",
                    station_key="2700000001",
                    station_name="淀川",
                    start_date="2025-01-05",
                    end_date="2025-01-05",
                ),
            ],
        )
    )

    assert result.errors == []
    assert len(result.csv_paths) == 2


def test_period_batch_export_allows_same_station_with_different_ranges(tmp_path):
    records = [
        _record(
            source="jma",
            station_key="11_62001",
            station_name="大阪",
            observed_at="2025-01-03 00:00:00",
            rainfall_mm=1.0,
        ),
        _record(
            source="jma",
            station_key="11_62001",
            station_name="大阪",
            observed_at="2025-01-04 00:00:00",
            rainfall_mm=2.0,
        ),
    ]
    save_records_parquet(records, build_parquet_path(tmp_path, "jma", "11_62001", 2025, month=1))

    result = run_rainfall_parquet_period_batch_export(
        RainfallParquetPeriodBatchExportInput(
            parquet_dir=str(tmp_path),
            output_dir=str(tmp_path / "csv"),
            targets=[
                RainfallParquetPeriodExportTarget(
                    source="jma",
                    station_key="11_62001",
                    station_name="大阪",
                    display_station_code="62001",
                    start_date="2025-01-03",
                    end_date="2025-01-03",
                ),
                RainfallParquetPeriodExportTarget(
                    source="jma",
                    station_key="11_62001",
                    station_name="大阪",
                    display_station_code="62001",
                    start_date="2025-01-04",
                    end_date="2025-01-04",
                ),
            ],
        )
    )

    assert result.errors == []
    assert len(result.csv_paths) == 2
    names = sorted(Path(path).name for path in result.csv_paths)
    assert names == [
        "jma_62001_大阪_2025-01-03.csv",
        "jma_62001_大阪_2025-01-04.csv",
    ]


def test_period_target_settings_csv_round_trip(tmp_path):
    targets = [
        RainfallParquetPeriodExportTarget(
            source="jma",
            station_key="11_62001",
            station_name="大阪",
            display_station_code="62001",
            start_date="2025-01-03",
            end_date="2025-01-04",
        ),
        RainfallParquetPeriodExportTarget(
            source="water_info",
            station_key="2700000001",
            station_name="淀川",
            display_station_code="2700000001",
            start_date="2025-01-05",
            end_date="2025-01-05",
        ),
    ]
    csv_path = export_period_targets_csv(tmp_path / "targets.csv", targets)
    loaded = load_period_targets_csv(csv_path)
    assert len(loaded) == 2
    assert loaded[0].station_key == "11_62001"
    assert str(loaded[0].start_date) == "2025-01-03"
    assert loaded[1].station_key == "2700000001"
    assert str(loaded[1].start_date) == "2025-01-05"
