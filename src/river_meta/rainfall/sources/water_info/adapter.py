from __future__ import annotations

from pathlib import Path
from typing import Callable

import pandas as pd

from water_info.infra.http_client import HEADERS, throttled_get
from water_info.service.flow_fetch import fetch_daily_dataframe_for_code, fetch_hourly_dataframe_for_code

from ...domain.models import RainfallQuery, RainfallRecord, WaterInfoStationInput
from ...domain.normalizer import infer_quality, normalize_observed_at, normalize_rainfall_value

LogFn = Callable[[str], None]
CancelFn = Callable[[], bool]


def _parse_station_name_from_file(file_name: str | Path | None, station_code: str) -> str:
    if not file_name:
        return ""
    stem = Path(file_name).stem
    parts = stem.split("_", 2)
    if len(parts) >= 3 and parts[0] == station_code:
        return parts[1]
    return ""


def _month_label(value: int) -> str:
    return f"{value}月"


def _build_year_month(query: RainfallQuery) -> tuple[str, str, str, str]:
    return (
        str(query.start_at.year),
        str(query.end_at.year),
        _month_label(query.start_at.month),
        _month_label(query.end_at.month),
    )


def _iterate_hourly_rows(df: pd.DataFrame) -> list[dict]:
    if df.empty:
        return []
    return df.to_dict(orient="records")


def _iterate_daily_rows(df: pd.DataFrame) -> list[dict]:
    if df.empty:
        return []
    rows = df.reset_index().rename(columns={"index": "date"})
    return rows.to_dict(orient="records")


def fetch_waterinfo_rainfall(
    stations: list[WaterInfoStationInput],
    query: RainfallQuery,
    *,
    include_raw: bool = True,
    log_warn: LogFn | None = None,
    should_stop: CancelFn | None = None,
) -> list[RainfallRecord]:
    warn = log_warn or (lambda _: None)
    if not stations:
        return []

    if query.interval not in {"1hour", "1day"}:
        raise ValueError("water_info adapter supports only 1hour/1day interval")

    year_start, year_end, month_start, month_end = _build_year_month(query)
    records: list[RainfallRecord] = []

    for station in stations:
        if _is_cancelled(should_stop):
            warn("water_info adapter cancelled")
            break
        station_name = station.station_name
        if query.interval == "1hour":
            df, file_name, value_col = fetch_hourly_dataframe_for_code(
                code=station.station_code,
                year_start=year_start,
                year_end=year_end,
                month_start=month_start,
                month_end=month_end,
                mode_type="U",
                throttled_get=throttled_get,
                headers=HEADERS,
                should_stop=should_stop,
            )
            if not station_name:
                station_name = _parse_station_name_from_file(file_name, station.station_code)
            if df is None or df.empty:
                continue

            for row in _iterate_hourly_rows(df):
                if _is_cancelled(should_stop):
                    warn("water_info adapter cancelled")
                    break
                observed_at = pd.to_datetime(row.get("datetime"), errors="coerce")
                if pd.isna(observed_at):
                    continue
                observed = normalize_observed_at(observed_at.to_pydatetime(), interval=query.interval)
                if observed < query.start_at or observed > query.end_at:
                    continue
                rainfall_mm = normalize_rainfall_value(row.get(value_col))
                records.append(
                    RainfallRecord(
                        source="water_info",
                        station_key=station.station_key,
                        station_name=station_name,
                        observed_at=observed,
                        interval=query.interval,
                        rainfall_mm=rainfall_mm,
                        quality=infer_quality(rainfall_mm),
                        raw=dict(row) if include_raw else {},
                    )
                )
            continue

        df, file_name, data_label, _ = fetch_daily_dataframe_for_code(
            code=station.station_code,
            year_start=year_start,
            year_end=year_end,
            month_start=month_start,
            month_end=month_end,
            mode_type="U",
            throttled_get=throttled_get,
            headers=HEADERS,
            should_stop=should_stop,
        )
        if not station_name:
            station_name = _parse_station_name_from_file(file_name, station.station_code)
        if df is None or df.empty:
            continue

        for row in _iterate_daily_rows(df):
            if _is_cancelled(should_stop):
                warn("water_info adapter cancelled")
                break
            observed_at = pd.to_datetime(row.get("date"), errors="coerce")
            if pd.isna(observed_at):
                continue
            observed = normalize_observed_at(observed_at.to_pydatetime(), interval=query.interval)
            if observed < query.start_at or observed > query.end_at:
                continue
            rainfall_mm = normalize_rainfall_value(row.get(data_label))
            records.append(
                RainfallRecord(
                    source="water_info",
                    station_key=station.station_key,
                    station_name=station_name,
                    observed_at=observed,
                    interval=query.interval,
                    rainfall_mm=rainfall_mm,
                    quality=infer_quality(rainfall_mm),
                    raw=dict(row) if include_raw else {},
                )
            )

    if not records:
        warn("water_info adapter returned no rainfall records")
    records.sort(key=lambda item: (item.station_key, item.observed_at))
    return records


def _is_cancelled(should_stop: CancelFn | None) -> bool:
    if should_stop is None:
        return False
    try:
        return bool(should_stop())
    except Exception:
        return False
