from __future__ import annotations

import pandas as pd

from hydrology_graphs.io.parquet_store import scan_parquet_catalog, scan_parquet_station_index


def test_scan_parquet_catalog_keeps_valid_rows_and_flags_invalid(tmp_path):
    valid = pd.DataFrame(
        {
            "source": ["jma"],
            "station_key": ["111"],
            "station_name": ["A"],
            "observed_at": ["2025-01-01 00:00:00"],
            "metric": ["rainfall"],
            "value": [1.0],
            "unit": ["mm"],
            "interval": ["1hour"],
            "quality": ["normal"],
        }
    )
    invalid = valid.copy()
    invalid.loc[0, "metric"] = "bad"
    valid.to_parquet(tmp_path / "valid.parquet", index=False)
    invalid.to_parquet(tmp_path / "invalid.parquet", index=False)

    catalog = scan_parquet_catalog(tmp_path)

    assert len(catalog.data) == 1
    assert catalog.invalid_files
    assert catalog.stations == [("jma", "111", "A")]
    assert catalog.base_dates == ["2025-01-01"]
    assert catalog.station_metric_labels == {("jma", "111"): ("雨量",)}


def test_scan_parquet_catalog_missing_columns(tmp_path):
    frame = pd.DataFrame({"source": ["jma"], "station_key": ["111"]})
    frame.to_parquet(tmp_path / "bad.parquet", index=False)

    catalog = scan_parquet_catalog(tmp_path)

    assert catalog.data.empty
    assert any("missing_columns" in ",".join(errors) for errors in catalog.invalid_files.values())


def test_scan_parquet_catalog_normalizes_legacy_jma_hourly_timestamps(tmp_path):
    legacy = pd.DataFrame(
        {
            "source": ["jma", "jma", "jma"],
            "station_key": ["111", "111", "111"],
            "station_name": ["A", "A", "A"],
            "observed_at": [
                "2026-02-02 01:00:00",
                "2026-02-02 02:00:00",
                "2026-02-02 23:59:59.999999",
            ],
            "metric": ["rainfall", "rainfall", "rainfall"],
            "value": [1.0, 2.0, 3.0],
            "unit": ["mm", "mm", "mm"],
            "interval": ["1hour", "1hour", "1hour"],
            "quality": ["normal", "normal", "normal"],
        }
    )
    legacy.to_parquet(tmp_path / "legacy.parquet", index=False)

    catalog = scan_parquet_catalog(tmp_path)
    observed = pd.to_datetime(catalog.data["observed_at"]).dt.strftime("%Y-%m-%d %H:%M:%S").tolist()

    assert observed == [
        "2026-02-02 01:00:00",
        "2026-02-02 02:00:00",
        "2026-02-03 00:00:00",
    ]


def test_scan_parquet_catalog_keeps_instantaneous_observed_at_when_period_end_missing(tmp_path):
    frame = pd.DataFrame(
        {
            "source": ["water_info"],
            "station_key": ["303"],
            "station_name": ["高幡橋"],
            "period_start_at": [None],
            "period_end_at": [None],
            "observed_at": ["2025-01-01 00:00:00"],
            "metric": ["water_level"],
            "value": [1.2],
            "unit": ["m"],
            "interval": ["1hour"],
            "quality": ["normal"],
        }
    )
    frame.to_parquet(tmp_path / "instant.parquet", index=False)

    catalog = scan_parquet_catalog(tmp_path)

    assert pd.Timestamp(catalog.data.loc[0, "observed_at"]) == pd.Timestamp("2025-01-01 00:00:00")
    assert pd.isna(catalog.data.loc[0, "period_end_at"])


def test_scan_parquet_station_index_infers_metric_labels_from_filenames(tmp_path):
    jma = pd.DataFrame(
        {
            "source": ["jma"],
            "station_key": ["111"],
            "station_name": ["A"],
        }
    )
    water_info_discharge = pd.DataFrame(
        {
            "source": ["water_info"],
            "station_key": ["303"],
            "station_name": ["高幡橋"],
        }
    )
    water_info_water_level = pd.DataFrame(
        {
            "source": ["water_info"],
            "station_key": ["303"],
            "station_name": ["高幡橋"],
        }
    )
    jma.to_parquet(tmp_path / "jma_111_1hour_202401_202402.parquet", index=False)
    water_info_discharge.to_parquet(tmp_path / "water_info_303_discharge_1hour_202401_202402.parquet", index=False)
    water_info_water_level.to_parquet(tmp_path / "water_info_303_water_level_1hour_202401_202402.parquet", index=False)

    catalog = scan_parquet_station_index(tmp_path)

    assert catalog.stations == [("jma", "111", "A"), ("water_info", "303", "高幡橋")]
    assert catalog.station_metric_labels[("jma", "111")] == ("雨量",)
    assert catalog.station_metric_labels[("water_info", "303")] == ("流量", "水位")
