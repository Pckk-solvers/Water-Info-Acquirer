"""Date helpers for water_info."""

from __future__ import annotations

from datetime import datetime


def month_floor(dt: datetime) -> datetime:
    """その月の月初(00:00)"""
    return datetime(dt.year, dt.month, 1)


def shift_month(dt: datetime, n: int) -> datetime:
    """月初を基準に n ヶ月シフトした月初"""
    y = dt.year + (dt.month - 1 + n) // 12
    m = (dt.month - 1 + n) % 12 + 1
    return datetime(y, m, 1)
