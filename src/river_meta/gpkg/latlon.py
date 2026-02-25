from __future__ import annotations

import re
from typing import Optional


DECIMAL_RE = re.compile(r"[+-]?\d+(?:\.\d+)?")
DMS_RE = re.compile(r"([0-9]+(?:\.[0-9]+)?)度([0-9]+(?:\.[0-9]+)?)分([0-9]+(?:\.[0-9]+)?)秒")


def dms_to_decimal(degree: float, minute: float, second: float, *, sign: int = 1) -> float:
    value = degree + minute / 60 + second / 3600
    return value * sign


def parse_decimal(value: str | float | int | None) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        match = DECIMAL_RE.search(text)
        if not match:
            return None
        try:
            return float(match.group(0))
        except ValueError:
            return None


def parse_japanese_dms(value: str | None, *, is_latitude: bool) -> Optional[float]:
    if not value:
        return None
    text = str(value).strip().replace("\u3000", " ")
    match = DMS_RE.search(text)
    if not match:
        return None

    degree = float(match.group(1))
    minute = float(match.group(2))
    second = float(match.group(3))

    sign = 1
    if is_latitude:
        if "南緯" in text:
            sign = -1
    else:
        if "西経" in text:
            sign = -1

    return dms_to_decimal(degree, minute, second, sign=sign)


def parse_latitude(value: str | float | int | None) -> Optional[float]:
    if isinstance(value, str) and ("度" in value and "分" in value and "秒" in value):
        dms = parse_japanese_dms(value, is_latitude=True)
        if dms is not None:
            return dms
    decimal = parse_decimal(value)
    if decimal is not None and -90 <= decimal <= 90:
        return decimal
    return parse_japanese_dms(str(value) if value is not None else None, is_latitude=True)


def parse_longitude(value: str | float | int | None) -> Optional[float]:
    if isinstance(value, str) and ("度" in value and "分" in value and "秒" in value):
        dms = parse_japanese_dms(value, is_latitude=False)
        if dms is not None:
            return dms
    decimal = parse_decimal(value)
    if decimal is not None and -180 <= decimal <= 180:
        return decimal
    return parse_japanese_dms(str(value) if value is not None else None, is_latitude=False)


def parse_latlon_pair(
    latitude: str | float | int | None,
    longitude: str | float | int | None,
) -> tuple[Optional[float], Optional[float]]:
    lat = parse_latitude(latitude)
    lon = parse_longitude(longitude)
    return lat, lon
