from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "src"))

from river_meta.rainfall.domain.models import RainfallDataset, RainfallRecord, WaterInfoStationInput
from river_meta.rainfall.domain.usecase_models import RainfallRunInput
from river_meta.rainfall.services.analyze import _fetch_waterinfo_year


def _record(rainfall_mm: float | None) -> RainfallRecord:
    return RainfallRecord(
        source="water_info",
        station_key="2700000001",
        station_name="大阪",
        observed_at=datetime(2025, 1, 1, 0, 0, 0),
        interval="1hour",
        rainfall_mm=rainfall_mm,
    )


def test_fetch_waterinfo_year_skip_save_when_no_valid_rainfall(monkeypatch, tmp_path):
    logs: list[str] = []
    save_calls = {"count": 0}

    def _fake_collect(stations, query, include_raw, logger, should_stop):  # noqa: ARG001
        return RainfallDataset(records=[_record(None), _record(None)], errors=[])

    def _fake_save(records, output_path):  # noqa: ARG001
        save_calls["count"] += 1
        return Path(output_path)

    monkeypatch.setattr("river_meta.rainfall.sources.fetch_water_info.parquet_exists", lambda *args, **kwargs: False)
    monkeypatch.setattr("river_meta.rainfall.sources.fetch_water_info.collect_waterinfo_with_resolved", _fake_collect)
    monkeypatch.setattr("river_meta.rainfall.sources.fetch_water_info.save_records_parquet", _fake_save)

    result = _fetch_waterinfo_year(
        station_obj_list=[WaterInfoStationInput(station_code="2700000001")],
        station_key="2700000001",
        year=2025,
        output_dir=str(tmp_path),
        config=RainfallRunInput(source="water_info", year=2025, waterinfo_station_codes=["2700000001"]),
        logger=logs.append,
        should_stop=None,
        all_errors=[],
        created_parquet_paths=[],
    )

    assert result is None
    assert save_calls["count"] == 0
    assert any("有効値なしのため保存スキップ" in message for message in logs)


def test_fetch_waterinfo_year_save_when_has_valid_rainfall(monkeypatch, tmp_path):
    logs: list[str] = []
    save_calls = {"count": 0}
    created_parquet_paths: list[Path] = []

    def _fake_collect(stations, query, include_raw, logger, should_stop):  # noqa: ARG001
        return RainfallDataset(records=[_record(None), _record(1.2)], errors=[])

    def _fake_save(records, output_path):  # noqa: ARG001
        save_calls["count"] += 1
        return Path(output_path)

    monkeypatch.setattr("river_meta.rainfall.sources.fetch_water_info.parquet_exists", lambda *args, **kwargs: False)
    monkeypatch.setattr("river_meta.rainfall.sources.fetch_water_info.collect_waterinfo_with_resolved", _fake_collect)
    monkeypatch.setattr("river_meta.rainfall.sources.fetch_water_info.save_records_parquet", _fake_save)

    result = _fetch_waterinfo_year(
        station_obj_list=[WaterInfoStationInput(station_code="2700000001")],
        station_key="2700000001",
        year=2025,
        output_dir=str(tmp_path),
        config=RainfallRunInput(source="water_info", year=2025, waterinfo_station_codes=["2700000001"]),
        logger=logs.append,
        should_stop=None,
        all_errors=[],
        created_parquet_paths=created_parquet_paths,
    )

    assert result is not None
    assert len(result) == 2
    assert save_calls["count"] == 1
    assert len(created_parquet_paths) == 1
    assert any("取得完了 (2件)" in message for message in logs)
