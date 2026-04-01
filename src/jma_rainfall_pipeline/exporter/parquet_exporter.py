from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any
import logging

import pandas as pd
from jma_rainfall_pipeline.fetcher.jma_codes_fetcher import fetch_station_codes

_UNIFIED_COLUMNS = [
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
]

logger = logging.getLogger(__name__)
_STATION_CACHE: dict[str, dict[str, str]] = {}


def _save_unified_records_parquet(records: list[dict[str, Any]], output_path: Path) -> Path:
    df = pd.DataFrame(records, columns=_UNIFIED_COLUMNS)
    if df.empty:
        df = pd.DataFrame(columns=_UNIFIED_COLUMNS)
    df["period_start_at"] = pd.to_datetime(df.get("period_start_at"), errors="coerce")
    df["period_end_at"] = pd.to_datetime(df.get("period_end_at"), errors="coerce")
    df["observed_at"] = pd.to_datetime(df["observed_at"], errors="coerce")
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
    if "datetime" in row and not pd.isna(row.get("datetime")):
        ts = pd.to_datetime(row.get("datetime"), errors="coerce")
        if not pd.isna(ts):
            if interval_label == "hourly":
                # 旧データの 23:59:59.999999 は 24:00 相当として扱う。
                if ts.hour == 23 and ts.minute == 59 and ts.second == 59 and ts.microsecond >= 999000:
                    ts = ts + pd.Timedelta(microseconds=1)
            elif interval_label == "10min":
                # 旧データ互換: 23:59:59.999999 を翌日 00:00 にそろえる。
                if ts.hour == 23 and ts.minute == 59 and ts.second == 59 and ts.microsecond >= 999000:
                    ts = ts + pd.Timedelta(microseconds=1)
            return ts
    if interval_label == "daily":
        ts = pd.to_datetime(row.get("date"), errors="coerce")
        if pd.isna(ts):
            return None
        return ts.replace(hour=0, minute=0, second=0, microsecond=0) + pd.Timedelta(days=1)
    if "date" in row and "time" in row:
        ts = pd.to_datetime(f"{row.get('date')} {row.get('time')}", errors="coerce")
        if not pd.isna(ts):
            return ts
    if "date" in row and "hour" in row:
        day = pd.to_datetime(row.get("date"), errors="coerce")
        hour_value = pd.to_numeric(row.get("hour"), errors="coerce")
        if not pd.isna(day) and not pd.isna(hour_value):
            hours = int(float(hour_value))
            minutes = int(round((float(hour_value) - hours) * 60))
            return day + pd.to_timedelta(hours=hours, minutes=minutes)
    return None


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
    interval = _to_interval_token(interval_label)
    file_name = (
        f"jma_{_safe_token(station_key)}_{interval}_"
        f"{start_date.strftime('%Y%m')}_{end_date.strftime('%Y%m')}.parquet"
    )
    out_path = output_dir / file_name

    value_col = "precipitation_total" if interval_label == "daily" else "precipitation"
    rows: list[dict[str, Any]] = []
    interval_hours = {"hourly": 1.0, "daily": 24.0, "10min": 10.0 / 60.0}[interval_label]
    for row in df.to_dict(orient="records"):
        period_end_at = _extract_observed_at(row, interval_label)
        if period_end_at is None or pd.isna(period_end_at):
            continue
        period_start_at = period_end_at - pd.to_timedelta(interval_hours, unit="h")
        value = pd.to_numeric(row.get(value_col), errors="coerce")
        value_float = None if pd.isna(value) else float(value)
        rows.append(
            {
                "source": "jma",
                "station_key": station_key,
                "station_name": station_name,
                "period_start_at": period_start_at.to_pydatetime(),
                "period_end_at": period_end_at.to_pydatetime(),
                "observed_at": period_end_at.to_pydatetime(),
                "metric": "rainfall",
                "value": value_float,
                "unit": "mm",
                "interval": interval,
                "quality": "normal" if value_float is not None else "missing",
            }
        )

    _save_unified_records_parquet(rows, out_path)
    return out_path
