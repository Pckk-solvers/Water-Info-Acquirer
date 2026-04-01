from __future__ import annotations

from datetime import datetime, timedelta
import math
from typing import Any

from .models import RainfallInterval, RainfallSource

_MISSING_TEXT = {"", "-", "--", "///", "×"}


def normalize_source_token(value: str) -> RainfallSource:
    token = (value or "").strip().lower()
    if token in {"jma", "weather"}:
        return "jma"
    if token in {"water_info", "waterinfo", "river"}:
        return "water_info"
    raise ValueError(f"Unsupported source: {value}")


def normalize_interval_token(value: str) -> RainfallInterval:
    token = (value or "").strip().lower()
    mapping = {
        "10min": "10min",
        "10m": "10min",
        "hourly": "1hour",
        "1hour": "1hour",
        "60min": "1hour",
        "daily": "1day",
        "1day": "1day",
    }
    if token not in mapping:
        raise ValueError(f"Unsupported interval: {value}")
    return mapping[token]


def normalize_rainfall_value(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, str):
        token = value.strip()
        if token in _MISSING_TEXT:
            return None
        try:
            return float(token.replace(",", ""))
        except ValueError:
            return None
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        number = float(value)
        if math.isnan(number):
            return None
        return number
    return None


def infer_quality(rainfall_mm: float | None) -> str:
    return "normal" if rainfall_mm is not None else "missing"


def normalize_observed_at(observed_at: datetime, *, interval: RainfallInterval) -> datetime:
    """互換API: 新契約では period_end_at 正規化として扱う。"""

    return normalize_period_end(observed_at, interval=interval)


def normalize_period_end(observed_at: datetime, *, interval: RainfallInterval) -> datetime:
    """生時刻を契約上の period_end_at へ正規化する。"""

    if interval == "1day":
        day_head = datetime(observed_at.year, observed_at.month, observed_at.day)
        if observed_at.time() == day_head.time():
            return day_head + timedelta(days=1)
        return day_head + timedelta(days=1)

    if (
        observed_at.hour == 23
        and observed_at.minute == 59
        and observed_at.second == 59
        and observed_at.microsecond > 0
    ):
        next_second = observed_at + timedelta(seconds=1)
        return next_second.replace(second=0, microsecond=0)

    return observed_at


def derive_period_start(period_end_at: datetime, *, interval: RainfallInterval) -> datetime:
    """period_end_at から period_start_at を導出する。"""

    if interval == "1day":
        return period_end_at - timedelta(days=1)
    if interval == "1hour":
        return period_end_at - timedelta(hours=1)
    return period_end_at - timedelta(minutes=10)


def normalize_period(observed_at: datetime, *, interval: RainfallInterval) -> tuple[datetime, datetime]:
    """生時刻から (period_start_at, period_end_at) を返す。"""

    period_end_at = normalize_period_end(observed_at, interval=interval)
    period_start_at = derive_period_start(period_end_at, interval=interval)
    return period_start_at, period_end_at
