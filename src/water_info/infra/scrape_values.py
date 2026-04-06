"""HTML scraping helpers for water_info."""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Iterable

import pandas as pd


@dataclass(frozen=True, slots=True)
class HourlyReading:
    datetime: pd.Timestamp
    value: float | None


def extract_font_values(soup) -> list[str]:
    return [f.get_text() for f in soup.select("td > font")]


def coerce_numeric_series(values: Iterable[str]):
    return pd.to_numeric(pd.Series(values), errors="coerce").tolist()


def extract_hourly_readings(soup, *, start_at: datetime) -> list[HourlyReading]:
    """HTML の行単位で時刻と値を対応付けて抽出する。"""

    readings = _extract_hourly_readings_from_date_rows(soup)
    if readings:
        return readings

    return _extract_hourly_readings_from_legacy_rows(soup, start_at=start_at)


def _extract_hourly_readings_from_date_rows(soup) -> list[HourlyReading]:
    """日付行 + 24 時間列の表から読み取る。"""

    readings: list[HourlyReading] = []

    for row in soup.select("tr"):
        cells = row.find_all(["td", "th"], recursive=False)
        if len(cells) < 25:
            continue

        day_text = cells[0].get_text(" ", strip=True)
        row_day = _extract_row_day(day_text)
        if row_day is None:
            continue

        for hour_index, cell in enumerate(cells[1:25], start=1):
            timestamp = row_day + pd.Timedelta(hours=hour_index)
            if hour_index == 24:
                timestamp = row_day + pd.Timedelta(days=1)
            value_text = cell.get_text(" ", strip=True)
            readings.append(HourlyReading(datetime=timestamp, value=_coerce_float(value_text)))

    return readings


def _extract_row_day(text: str) -> pd.Timestamp | None:
    token = text.strip()
    if not token:
        return None
    ts = pd.to_datetime(token, errors="coerce")
    if not isinstance(ts, pd.Timestamp) or pd.isna(ts):
        return None
    if ts.year < 1900:
        return None
    return ts.normalize()


def _extract_hourly_readings_from_legacy_rows(soup, *, start_at: datetime) -> list[HourlyReading]:
    """旧形式の時刻トークン行から読み取る。"""

    readings: list[HourlyReading] = []
    current_day = pd.Timestamp(start_at).normalize()
    previous_dt = pd.Timestamp(start_at)

    for row in soup.select("tr"):
        cells = row.find_all(["td", "th"], recursive=False)
        if not cells:
            continue

        cell_texts = [cell.get_text(" ", strip=True) for cell in cells]
        value_text = _pick_value_text(cells, cell_texts)
        if value_text is None:
            continue

        row_dt = _resolve_legacy_row_datetime(cell_texts, current_day=current_day, previous_dt=previous_dt)
        if row_dt is None:
            continue

        if row_dt.normalize() != current_day:
            current_day = row_dt.normalize()
        previous_dt = row_dt
        readings.append(HourlyReading(datetime=row_dt, value=_coerce_float(value_text)))

    return readings


def _pick_value_text(cells, cell_texts: list[str]) -> str | None:
    """行内から値らしい文字列を1つ選ぶ。"""

    font_texts: list[str] = []
    for cell in cells:
        font_texts.extend(font.get_text(strip=True) for font in cell.select("font"))

    candidates = font_texts if font_texts else cell_texts
    for text in reversed(candidates):
        if _looks_like_value(text):
            return text
    return None


def _resolve_legacy_row_datetime(
    cell_texts: list[str],
    *,
    current_day: pd.Timestamp,
    previous_dt: pd.Timestamp,
) -> pd.Timestamp | None:
    """行テキストから日時を推定する。"""

    full_dt = _extract_full_datetime(cell_texts)
    if full_dt is not None:
        return full_dt

    time_token = _extract_time_token(cell_texts)
    if time_token is None:
        return previous_dt + pd.Timedelta(hours=1)

    hour, minute = time_token
    if hour == 24 and minute == 0:
        return current_day + pd.Timedelta(days=1)

    candidate = current_day + pd.Timedelta(hours=hour, minutes=minute)
    if candidate <= previous_dt:
        candidate = current_day + pd.Timedelta(days=1, hours=hour, minutes=minute)
    return candidate


def _extract_full_datetime(cell_texts: list[str]) -> pd.Timestamp | None:
    for text in cell_texts:
        if not any(sep in text for sep in ("-", "/", " ")):
            continue
        ts = pd.to_datetime(text, errors="coerce")
        if isinstance(ts, pd.Timestamp) and not pd.isna(ts):
            if ts.year >= 1900:
                return ts
    return None


def _extract_time_token(cell_texts: list[str]) -> tuple[int, int] | None:
    for text in cell_texts:
        match = re.fullmatch(r"(?P<hour>\d{1,2}):(?P<minute>\d{2})", text.strip())
        if not match:
            continue
        hour = int(match.group("hour"))
        minute = int(match.group("minute") or 0)
        if hour > 24 or minute > 59:
            continue
        return hour, minute
    return None


def _looks_like_value(text: str) -> bool:
    token = text.strip()
    if not token:
        return False
    if _extract_time_token([token]) is not None:
        return False
    numeric = pd.to_numeric(pd.Series([token]), errors="coerce").iloc[0]
    return not pd.isna(numeric)


def _coerce_float(text: str) -> float | None:
    value = pd.to_numeric(pd.Series([text]), errors="coerce").iloc[0]
    if pd.isna(value):
        return None
    return float(value)
