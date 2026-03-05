from __future__ import annotations

from datetime import datetime, time, timedelta
from typing import Callable

import pandas as pd

from jma_rainfall_pipeline.fetcher.fetcher import Fetcher
from jma_rainfall_pipeline.logger.app_logger import set_runtime_log_options, setup_logging
from jma_rainfall_pipeline.parser import parse_html

from .models import JMAStationInput, RainfallQuery, RainfallRecord
from .normalizer import infer_quality, normalize_observed_at, normalize_rainfall_value

_NO_DATA_MARKERS = (
    "データは存在しません",
    "データはありません",
    "データが見つかりません",
)
_ERROR_PAGE_MARKERS = (
    "ページを表示することが出来ませんでした。",
    "ページを表示することが出来ませんでした",
)

LogFn = Callable[[str], None]
CancelFn = Callable[[], bool]


def _to_timedelta(interval: str) -> timedelta:
    if interval == "1day":
        return timedelta(days=1)
    if interval == "1hour":
        return timedelta(hours=1)
    return timedelta(minutes=10)


def _to_frequency(interval: str) -> str:
    if interval == "1day":
        return "daily"
    if interval == "1hour":
        return "hourly"
    return "10min"


def _extract_datetime(row: dict, sample_dt: datetime, interval: str) -> datetime | None:
    dt_value = row.get("datetime")
    if dt_value is not None and not pd.isna(dt_value):
        parsed = pd.to_datetime(dt_value, errors="coerce")
        if not pd.isna(parsed):
            return parsed.to_pydatetime()

    date_value = row.get("date")
    base_date = pd.to_datetime(date_value, errors="coerce")
    if pd.isna(base_date):
        base_date = pd.Timestamp(sample_dt.date())

    time_value = row.get("time")
    if time_value is not None and not pd.isna(time_value):
        parsed = pd.to_datetime(f"{base_date.date()} {time_value}", errors="coerce")
        if not pd.isna(parsed):
            return parsed.to_pydatetime()

    hour_value = row.get("hour")
    if hour_value is not None and not pd.isna(hour_value):
        hour_float = float(hour_value)
        hours = int(hour_float)
        minutes = int(round((hour_float - hours) * 60))
        return (base_date + pd.Timedelta(hours=hours, minutes=minutes)).to_pydatetime()

    if interval == "1day":
        return datetime.combine(base_date.date(), time.min)
    return sample_dt


def fetch_jma_rainfall(
    stations: list[JMAStationInput],
    query: RainfallQuery,
    *,
    base_url: str = "https://www.data.jma.go.jp/",
    include_raw: bool = True,
    log_warn: LogFn | None = None,
    should_stop: CancelFn | None = None,
    jma_log_level: str | None = None,
    jma_enable_log_output: bool | None = None,
) -> list[RainfallRecord]:
    warn = log_warn or (lambda _: None)
    if not stations:
        return []

    if jma_log_level is not None or jma_enable_log_output is not None:
        set_runtime_log_options(
            level=jma_log_level,
            enable_log_output=jma_enable_log_output,
            logger_scope="jma",
        )
        setup_logging(
            level_override=jma_log_level,
            enable_log_output=True if jma_enable_log_output is None else bool(jma_enable_log_output),
            logger_scope="jma",
        )

    fetcher = Fetcher(base_url=base_url, interval=_to_timedelta(query.interval))
    frequency = _to_frequency(query.interval)
    station_tuples = [(s.prefecture_code, s.block_number, s.obs_type) for s in stations]
    station_map = {(s.prefecture_code, s.block_number): s for s in stations}

    records: list[RainfallRecord] = []
    for (prec_no, block_no), sample_dt, html, _ in fetcher.schedule_fetch(
        station_tuples,
        query.start_at,
        query.end_at,
    ):
        if _is_cancelled(should_stop):
            warn("jma adapter cancelled")
            break
        if any(marker in html for marker in _NO_DATA_MARKERS):
            continue
        if any(marker in html for marker in _ERROR_PAGE_MARKERS):
            warn(
                f"JMA unavailable page ({prec_no}-{block_no} {sample_dt:%Y-%m-%d})"
            )
            continue

        station = station_map.get((str(prec_no), str(block_no)))
        obs_type = station.obs_type if station else "a1"

        try:
            df = parse_html(html, frequency, sample_dt.date(), obs_type=obs_type)
        except Exception as exc:  # noqa: BLE001
            warn(f"JMA parse failed ({prec_no}-{block_no}): {type(exc).__name__}: {exc}")
            continue

        if df is None or df.empty:
            continue

        station_name = station.station_name if station else ""
        station_key = station.station_key if station else f"{prec_no}_{block_no}"

        for row in df.to_dict(orient="records"):
            if _is_cancelled(should_stop):
                warn("jma adapter cancelled")
                break
            observed_at = _extract_datetime(row, sample_dt, query.interval)
            if observed_at is None:
                continue
            observed_at = normalize_observed_at(observed_at, interval=query.interval)
            observed_at = _align_hourly_timestamp_to_waterinfo(observed_at, interval=query.interval)
            if observed_at < query.start_at or observed_at > query.end_at:
                continue

            rainfall_mm = normalize_rainfall_value(row.get("precipitation"))
            if not station_name:
                station_name = str(row.get("station_name", "") or "")

            records.append(
                RainfallRecord(
                    source="jma",
                    station_key=station_key,
                    station_name=station_name,
                    observed_at=observed_at,
                    interval=query.interval,
                    rainfall_mm=rainfall_mm,
                    quality=infer_quality(rainfall_mm),
                    raw=dict(row) if include_raw else {},
                )
            )

    records.sort(key=lambda item: (item.station_key, item.observed_at))
    return records


def _align_hourly_timestamp_to_waterinfo(observed_at: datetime, *, interval: str) -> datetime:
    if interval == "1hour":
        return observed_at - timedelta(hours=1)
    return observed_at


def _is_cancelled(should_stop: CancelFn | None) -> bool:
    if should_stop is None:
        return False
    try:
        return bool(should_stop())
    except Exception:
        return False
