from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, Callable

from river_meta.rainfall.domain.models import JMAStationInput, RainfallDataset, RainfallQuery
from river_meta.rainfall.storage.parquet_store import (
    build_parquet_path,
    load_and_concat_monthly_parquets,
    migrate_legacy_jma_parquets,
    parquet_exists,
    save_records_parquet,
)

from river_meta.rainfall.support.common import CancelFn, LogFn, append_cancelled_once, is_cancelled

if TYPE_CHECKING:
    import pandas as pd
    from river_meta.rainfall.domain.usecase_models import RainfallRunInput


def fetch_jma_rainfall(*args, **kwargs):
    from river_meta.rainfall.sources.jma.adapter import fetch_jma_rainfall as _fetch_jma_rainfall

    return _fetch_jma_rainfall(*args, **kwargs)


def collect_jma_with_resolved(
    stations: list[JMAStationInput],
    query: RainfallQuery,
    include_raw: bool,
    logger: LogFn,
    should_stop: CancelFn | None,
    jma_log_level: str | None,
    jma_enable_log_output: bool | None,
) -> RainfallDataset:
    fetch_fn = fetch_jma_rainfall
    try:
        records = fetch_fn(
            stations=stations,
            query=query,
            include_raw=include_raw,
            log_warn=logger,
            should_stop=should_stop,
            jma_log_level=jma_log_level,
            jma_enable_log_output=jma_enable_log_output,
        )
        return RainfallDataset(records=records, errors=[])
    except Exception as exc:  # noqa: BLE001
        message = f"jma:{type(exc).__name__}: {exc}"
        logger(message)
        return RainfallDataset(records=[], errors=[message])


def fetch_jma_year_monthly(
    *,
    station_obj_list: list[JMAStationInput],
    station_key: str,
    year: int,
    output_dir: str,
    config: "RainfallRunInput",
    logger: LogFn,
    should_stop: CancelFn | None,
    all_errors: list[str],
    records_counter: Callable[[int], None],
    created_parquet_paths: list[Path],
    collect_fn=None,
    parquet_exists_fn=None,
    save_records_parquet_fn=None,
    migrate_legacy_jma_parquets_fn=None,
    load_and_concat_monthly_parquets_fn=None,
) -> "pd.DataFrame | None":
    import calendar

    if collect_fn is None:
        collect_fn = collect_jma_with_resolved
    if parquet_exists_fn is None:
        parquet_exists_fn = parquet_exists
    if save_records_parquet_fn is None:
        save_records_parquet_fn = save_records_parquet
    if migrate_legacy_jma_parquets_fn is None:
        migrate_legacy_jma_parquets_fn = migrate_legacy_jma_parquets
    if load_and_concat_monthly_parquets_fn is None:
        load_and_concat_monthly_parquets_fn = load_and_concat_monthly_parquets

    block_number = station_obj_list[0].block_number if station_obj_list else ""
    if block_number and block_number != station_key:
        migrated = migrate_legacy_jma_parquets_fn(output_dir, block_number, station_key, year)
        if migrated:
            logger(f"[Parquet] 旧フォーマットから {migrated} ファイルをリネームしました (観測所={station_key}, 年={year})")

    for month in range(1, 13):
        if is_cancelled(should_stop):
            append_cancelled_once(all_errors)
            break

        if parquet_exists_fn(output_dir, "jma", station_key, year, month=month):
            logger(f"[JMA] 観測所={station_key} {year}/{month:02d} キャッシュあり")
            continue

        last_day = calendar.monthrange(year, month)[1]
        query_start = datetime(year, month, 1, 0, 0, 0)
        query_end = datetime(year, month, last_day, 23, 59, 59)
        query = RainfallQuery(start_at=query_start, end_at=query_end, interval="1hour")

        logger(f"[JMA] 観測所={station_key} {year}/{month:02d} データ取得中...")
        part = collect_fn(
            station_obj_list,
            query,
            config.include_raw,
            logger,
            should_stop,
            config.jma_log_level,
            config.jma_enable_log_output,
        )
        all_errors.extend(part.errors)
        if is_cancelled(should_stop) or "cancelled" in part.errors:
            append_cancelled_once(all_errors)
            break

        if not part.records:
            logger(f"[JMA] 観測所={station_key} {year}/{month:02d} データなし")
            continue

        logger(f"[JMA] 観測所={station_key} {year}/{month:02d} 取得完了 ({len(part.records)}件)")
        records_counter(len(part.records))

        pq_path = build_parquet_path(output_dir, "jma", station_key, year, month=month)
        save_records_parquet_fn(part.records, pq_path)
        created_parquet_paths.append(pq_path)
        logger(f"[Parquet] 保存完了: {pq_path.name}")

    combined = load_and_concat_monthly_parquets_fn(output_dir, "jma", station_key, year)
    if combined.empty:
        return None
    return combined
