from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from river_meta.rainfall.waterinfo_adapter import fetch_waterinfo_rainfall
from river_meta.rainfall.models import RainfallDataset, RainfallQuery, WaterInfoStationInput
from river_meta.rainfall.parquet_store import build_parquet_path, load_records_parquet, parquet_exists, save_records_parquet

from .rainfall_common import CancelFn, LogFn, append_cancelled_once, is_cancelled

if TYPE_CHECKING:
    import pandas as pd
    from river_meta.services.rainfall import RainfallRunInput


def collect_waterinfo_with_resolved(
    stations: list[WaterInfoStationInput],
    query: RainfallQuery,
    include_raw: bool,
    logger: LogFn,
    should_stop: CancelFn | None,
) -> RainfallDataset:
    fetch_fn = fetch_waterinfo_rainfall
    try:
        records = fetch_fn(
            stations=stations,
            query=query,
            include_raw=include_raw,
            log_warn=logger,
            should_stop=should_stop,
        )
        return RainfallDataset(records=records, errors=[])
    except Exception as exc:  # noqa: BLE001
        message = f"water_info:{type(exc).__name__}: {exc}"
        logger(message)
        return RainfallDataset(records=[], errors=[message])


def fetch_waterinfo_year(
    *,
    station_obj_list: list[WaterInfoStationInput],
    station_key: str,
    year: int,
    output_dir: str,
    config: "RainfallRunInput",
    logger: LogFn,
    should_stop: CancelFn | None,
    all_errors: list[str],
    created_parquet_paths: list[Path],
    collect_fn=None,
    parquet_exists_fn=None,
    load_records_parquet_fn=None,
    save_records_parquet_fn=None,
) -> "pd.DataFrame | None":
    if collect_fn is None:
        collect_fn = collect_waterinfo_with_resolved
    if parquet_exists_fn is None:
        parquet_exists_fn = parquet_exists
    if load_records_parquet_fn is None:
        load_records_parquet_fn = load_records_parquet
    if save_records_parquet_fn is None:
        save_records_parquet_fn = save_records_parquet
    if is_cancelled(should_stop):
        append_cancelled_once(all_errors)
        return None

    pq_path = build_parquet_path(output_dir, "water_info", station_key, year)

    if parquet_exists_fn(output_dir, "water_info", station_key, year):
        logger(f"[水文水質DB] 観測所={station_key} 年={year} キャッシュから読み込み")
        return load_records_parquet_fn(pq_path)

    logger(f"[水文水質DB] 観測所={station_key} 年={year} データ取得中...")
    query_start = datetime(year, 1, 1, 0, 0, 0)
    query_end = datetime(year, 12, 31, 23, 59, 59)
    query = RainfallQuery(start_at=query_start, end_at=query_end, interval="1hour")

    part = collect_fn(
        station_obj_list,
        query,
        config.include_raw,
        logger,
        should_stop,
    )
    all_errors.extend(part.errors)
    if is_cancelled(should_stop) or "cancelled" in part.errors:
        append_cancelled_once(all_errors)
        return None

    if not part.records:
        logger(f"[水文水質DB] 観測所={station_key} 年={year} データなし")
        return None

    valid_rainfall_count = sum(1 for record in part.records if record.rainfall_mm is not None)
    if valid_rainfall_count == 0:
        logger(f"[水文水質DB] 観測所={station_key} 年={year} 有効値なしのため保存スキップ")
        return None

    logger(f"[水文水質DB] 観測所={station_key} 年={year} 取得完了 ({len(part.records)}件)")
    save_records_parquet_fn(part.records, pq_path)
    created_parquet_paths.append(pq_path)
    logger(f"[Parquet] 保存完了: {pq_path.name}")

    source_df = part.to_dataframe()
    del part
    return source_df
