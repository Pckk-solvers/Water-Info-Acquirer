"""Fetch helpers for water_info."""

from __future__ import annotations

from typing import Iterable

from .http_html import fetch_html, parse_html
from .scrape_station import extract_station_name
from .scrape_values import extract_font_values, coerce_numeric_series


def fetch_station_name(throttled_get, headers: dict, url: str) -> str:
    html = fetch_html(throttled_get, headers, url)
    soup = parse_html(html)
    return extract_station_name(soup)


def fetch_font_values(throttled_get, headers: dict, url: str) -> list[str]:
    html = fetch_html(throttled_get, headers, url)
    soup = parse_html(html)
    return extract_font_values(soup)


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
) -> list[float | str]:
    raw_values: list[str] = []
    values: list[float | str] = []
    for url in urls:
        raw_values = fetch_font_values(throttled_get, headers, url)
        chunk = coerce_hourly_values(raw_values)
        if drop_last_each and chunk:
            chunk.pop()
        values.extend(chunk)
    if drop_last and values:
        values.pop()
    return values


def fetch_daily_values(throttled_get, headers: dict, url: str):
    raw = fetch_font_values(throttled_get, headers, url)
    return coerce_numeric_series(raw)
