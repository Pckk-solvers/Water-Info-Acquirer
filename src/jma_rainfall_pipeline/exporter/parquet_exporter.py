from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any, cast
import logging

import pandas as pd
from jma_rainfall_pipeline.fetcher.jma_codes_fetcher import fetch_station_codes

_UNIFIED_COLUMNS = (
    "source",
    "station_key",
    "station_name",
    "period_start_at",
    "period_end_at",
    "observed_at",
    "metric",
    "value",
    "unit",
    "interval",
    "quality",
)

logger = logging.getLogger(__name__)
_STATION_CACHE: dict[str, dict[str, str]] = {}


def _save_unified_records_parquet(records: list[dict[str, Any]], output_path: Path) -> Path:
    df = pd.DataFrame(records, columns=pd.Index(_UNIFIED_COLUMNS))
    if df.empty:
        df = pd.DataFrame(columns=pd.Index(_UNIFIED_COLUMNS))
    period_start_series = cast(pd.Series, df["period_start_at"])
    period_end_series = cast(pd.Series, df["period_end_at"])
    observed_series = cast(pd.Series, df["observed_at"])
    df["period_start_at"] = pd.to_datetime(period_start_series, errors="coerce")
    df["period_end_at"] = pd.to_datetime(period_end_series, errors="coerce")
    df["observed_at"] = pd.to_datetime(observed_series, errors="coerce")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(output_path, engine="pyarrow", index=False)
    return output_path


def _safe_token(value: str) -> str:
    return str(value).replace("/", "_").replace("\\", "_")


def _to_interval_token(interval_label: str) -> str:
    if interval_label == "daily":
        return "1day"
    if interval_label == "hourly":
        return "1hour"
    return "10min"


def _resolve_station_name(prec_no: str, block_no: str) -> str:
    prec_key = str(prec_no).zfill(2)
    block_key = str(block_no).strip()
    if prec_key not in _STATION_CACHE:
        station_map: dict[str, str] = {}
        try:
            records = fetch_station_codes(prec_key)
            for rec in records:
                b = str(rec.get("block_no", "")).strip()
                n = str(rec.get("station", "")).strip()
                if b:
                    station_map[b] = n
        except Exception as exc:  # pragma: no cover - ネットワーク依存のフォールバック
            logger.warning("観測所名解決に失敗しました (prec_no=%s): %s", prec_key, exc)
        _STATION_CACHE[prec_key] = station_map
    return _STATION_CACHE.get(prec_key, {}).get(block_key, "")


def _to_codepoints(text: str) -> str:
    if not text:
        return ""
    return " ".join(f"U+{ord(ch):04X}" for ch in text)


def _extract_observed_at(row: dict[str, Any], interval_label: str) -> pd.Timestamp | None:
    raw_datetime = row.get("datetime")
    if raw_datetime is not None and not pd.isna(raw_datetime):
        ts = pd.to_datetime(cast(Any, raw_datetime), errors="coerce")
        if isinstance(ts, pd.Timestamp):
            if interval_label == "hourly":
                # 旧データの 23:59:59.999999 は 24:00 相当として扱う。
                if ts.hour == 23 and ts.minute == 59 and ts.second == 59 and ts.microsecond >= 999000:
                    ts = ts + pd.Timedelta(microseconds=1)
            elif interval_label == "10min":
                # 旧データ互換: 23:59:59.999999 を翌日 00:00 にそろえる。
                if ts.hour == 23 and ts.minute == 59 and ts.second == 59 and ts.microsecond >= 999000:
                    ts = ts + pd.Timedelta(microseconds=1)
            return cast(pd.Timestamp, ts)
    if interval_label == "daily":
        ts = pd.to_datetime(cast(Any, row.get("date")), errors="coerce")
        if not isinstance(ts, pd.Timestamp):
            return None
        return cast(pd.Timestamp, ts.replace(hour=0, minute=0, second=0, microsecond=0) + pd.Timedelta(days=1))
    if "date" in row and "time" in row:
        ts = pd.to_datetime(f"{row.get('date')} {row.get('time')}", errors="coerce")
        if isinstance(ts, pd.Timestamp):
            return cast(pd.Timestamp, ts)
    if "date" in row and "hour" in row:
        day = pd.to_datetime(cast(Any, row.get("date")), errors="coerce")
        try:
            hour_float = float(cast(Any, row.get("hour")))
        except (TypeError, ValueError):
            return None
        if isinstance(day, pd.Timestamp):
            hours = int(hour_float)
            minutes = int(round((hour_float - hours) * 60))
            return cast(pd.Timestamp, day + pd.Timedelta(hours=hours, minutes=minutes))
    return None


def build_normalized_time_frame(df: pd.DataFrame, interval_label: str) -> pd.DataFrame:
    """CSV/Excel/Parquet 共通の時刻契約へ正規化する。"""

    work = df.copy()
    if work.empty:
        work["period_start_at"] = pd.Series(dtype="datetime64[ns]")
        work["period_end_at"] = pd.Series(dtype="datetime64[ns]")
        work["observed_at"] = pd.Series(dtype="datetime64[ns]")
        return work

    period_end_list: list[pd.Timestamp | None] = []
    for row in work.to_dict(orient="records"):
        ts = _extract_observed_at(row, interval_label)
        period_end_list.append(ts)

    period_end_series = pd.to_datetime(pd.Series(period_end_list, index=work.index), errors="coerce")
    work["period_end_at"] = period_end_series
    if interval_label == "daily":
        work["period_start_at"] = period_end_series - pd.to_timedelta(1, unit="d")
    elif interval_label == "hourly":
        work["period_start_at"] = period_end_series - pd.to_timedelta(1, unit="h")
    else:
        work["period_start_at"] = period_end_series - pd.to_timedelta(10, unit="m")
    work["observed_at"] = work["period_end_at"]
    return work


def export_weather_parquet(
    df: pd.DataFrame,
    *,
    prec_no: str,
    block_no: str,
    interval_label: str,
    start_date: date,
    end_date: date,
    output_dir: Path,
) -> Path:
    rows = build_jma_unified_records(
        df,
        prec_no=str(prec_no),
        block_no=str(block_no),
        interval_label=interval_label,
    )
    interval = _to_interval_token(interval_label)
    file_name = (
        f"jma_{_safe_token(f'{prec_no}_{block_no}')}_{interval}_"
        f"{start_date.strftime('%Y%m')}_{end_date.strftime('%Y%m')}.parquet"
    )
    out_path = output_dir / file_name
    _save_unified_records_parquet(rows, out_path)
    return out_path


def build_jma_unified_records(
    df: pd.DataFrame,
    *,
    prec_no: str,
    block_no: str,
    interval_label: str,
) -> list[dict[str, Any]]:
    station_key = f"{prec_no}_{block_no}"
    station_name = _resolve_station_name(prec_no, block_no)
    if station_name:
        logger.debug(
            "Resolved JMA station_name: station_key=%s name=%s codepoints=%s",
            station_key,
            station_name,
            _to_codepoints(station_name),
        )
    else:
        logger.debug("Resolved JMA station_name is empty: station_key=%s", station_key)
    value_col = "precipitation_total" if interval_label == "daily" else "precipitation"
    rows: list[dict[str, Any]] = []
    normalized = build_normalized_time_frame(df, interval_label)
    interval = _to_interval_token(interval_label)
    for row in normalized.to_dict(orient="records"):
        period_start_at = pd.to_datetime(cast(Any, row.get("period_start_at")), errors="coerce")
        period_end_at = pd.to_datetime(cast(Any, row.get("period_end_at")), errors="coerce")
        observed_at = pd.to_datetime(cast(Any, row.get("observed_at")), errors="coerce")
        if not isinstance(period_start_at, pd.Timestamp):
            continue
        if not isinstance(period_end_at, pd.Timestamp):
            continue
        if not isinstance(observed_at, pd.Timestamp):
            continue
        numeric_value = pd.to_numeric(row.get(value_col), errors="coerce")
        value_float = float(numeric_value) if isinstance(numeric_value, (int, float)) and not pd.isna(numeric_value) else None
        rows.append(
            {
                "source": "jma",
                "station_key": station_key,
                "station_name": station_name,
                "period_start_at": period_start_at.to_pydatetime(),
                "period_end_at": period_end_at.to_pydatetime(),
                "observed_at": observed_at.to_pydatetime(),
                "metric": "rainfall",
                "value": value_float,
                "unit": "mm",
                "interval": interval,
                "quality": "normal" if value_float is not None else "missing",
            }
        )
    return rows
