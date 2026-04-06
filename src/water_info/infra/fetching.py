"""Fetch helpers for water_info."""

from __future__ import annotations

from typing import Callable, Iterable

from .http_html import fetch_html, parse_html
from .scrape_station import extract_station_name
from .scrape_values import HourlyReading, coerce_numeric_series, extract_font_values, extract_hourly_readings


def fetch_station_name(throttled_get, headers: dict, url: str, should_stop=None) -> str:
    html = fetch_html(throttled_get, headers, url, should_stop=should_stop)
    soup = parse_html(html)
    return extract_station_name(soup)


def fetch_font_values(throttled_get, headers: dict, url: str, should_stop=None) -> list[str]:
    html = fetch_html(throttled_get, headers, url, should_stop=should_stop)
    soup = parse_html(html)
    return extract_font_values(soup)


def fetch_hourly_readings(
    throttled_get,
    headers: dict,
    url: str,
    *,
    start_at,
    should_stop=None,
) -> list[HourlyReading]:
    html = fetch_html(throttled_get, headers, url, should_stop=should_stop)
    soup = parse_html(html)
    return extract_hourly_readings(soup, start_at=start_at)


def coerce_hourly_values(values: Iterable[str]) -> list[float | str]:
    coerced: list[float | str] = []
    for val in values:
        try:
            coerced.append(float(val))
        except Exception:
            coerced.append("")
    return coerced


def fetch_hourly_values(
    throttled_get,
    headers: dict,
    urls: Iterable[str],
    drop_last: bool = False,
    drop_last_each: bool = False,
    on_chunk: Callable[[], None] | None = None,
    should_stop=None,
) -> list[float | str]:
    raw_values: list[str] = []
    values: list[float | str] = []
    for url in urls:
        if should_stop is None:
            raw_values = fetch_font_values(throttled_get, headers, url)
        else:
            try:
                raw_values = fetch_font_values(throttled_get, headers, url, should_stop=should_stop)
            except TypeError:
                # 既存テスト/モック互換: should_stop 非対応シグネチャを許容
                raw_values = fetch_font_values(throttled_get, headers, url)
        chunk = coerce_hourly_values(raw_values)
        if drop_last_each and chunk:
            chunk.pop()
        values.extend(chunk)
        if on_chunk:
            on_chunk()
    if drop_last and values:
        values.pop()
    return values


def fetch_daily_values(throttled_get, headers: dict, url: str, should_stop=None):
    if should_stop is None:
        raw = fetch_font_values(throttled_get, headers, url)
    else:
        try:
            raw = fetch_font_values(throttled_get, headers, url, should_stop=should_stop)
        except TypeError:
            raw = fetch_font_values(throttled_get, headers, url)
    return coerce_numeric_series(raw)
