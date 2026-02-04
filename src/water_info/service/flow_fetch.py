"""Fetch flows for water_info."""

from __future__ import annotations

import calendar
from datetime import datetime
from pathlib import Path

import pandas as pd

from ..infra.dataframe_utils import build_daily_dataframe, build_hourly_dataframe
from ..infra.fetching import fetch_daily_values, fetch_hourly_values, fetch_station_name
from ..infra.url_builder import build_daily_base, build_daily_base_url, build_daily_url, build_hourly_base, build_hourly_url

_MONTH_LIST = [
    "0101", "0201", "0301", "0401", "0501", "0601",
    "0701", "0801", "0901", "1001", "1101", "1201",
]
_MONTH_DIC = {
    "1月": 0,
    "2月": 1,
    "3月": 2,
    "4月": 3,
    "5月": 4,
    "6月": 5,
    "7月": 6,
    "8月": 7,
    "9月": 8,
    "10月": 9,
    "11月": 10,
    "12月": 11,
}


def fetch_hourly_dataframe_for_code(
    code: str,
    year_start: str,
    year_end: str,
    month_start: str,
    month_end: str,
    mode_type: str,
    throttled_get,
    headers: dict,
    progress_callback=None,
):
    if mode_type == "S":
        value_col = "水位"
        file_suffix = "_WH.xlsx"
    elif mode_type == "R":
        value_col = "流量"
        file_suffix = "_QH.xlsx"
    elif mode_type == "U":
        value_col = "雨量"
        file_suffix = "_RH.xlsx"
    else:
        return None, None, None

    num, mode_str = build_hourly_base(mode_type)

    url_month = []
    new_month = []
    i = _MONTH_DIC[month_start]
    total = (_MONTH_DIC[month_end] - _MONTH_DIC[month_start] + 1) + (int(year_end) - int(year_start)) * 12
    for _ in range(total):
        new_month.append(_MONTH_LIST[i])
        i = (i + 1) % 12

    kari_year = int(year_start)
    for m in new_month:
        url_month.append(f"{kari_year}{m}")
        if m == "1201":
            kari_year += 1

    first_date = url_month[0]
    first_url = build_hourly_url(code, num, mode_str, first_date, f"{year_end}1231")
    station_name = fetch_station_name(throttled_get, headers, first_url)
    if progress_callback:
        progress_callback(increment=False, station_name=station_name)

    out_dir = Path("water_info")
    out_dir.mkdir(parents=True, exist_ok=True)
    file_name = out_dir / f"{code}_{station_name}_{year_start}年{month_start}-{year_end}年{month_end}{file_suffix}"

    url_list = [build_hourly_url(code, num, mode_str, um, f"{year_end}1231") for um in url_month]
    values = fetch_hourly_values(
        throttled_get,
        headers,
        url_list,
        drop_last_each=mode_type in ["S", "U"],
        on_chunk=lambda: progress_callback(increment=True) if progress_callback else None,
    )

    year_start_i = int(year_start)
    month_start_i = int(new_month[0][:2])
    start_date = datetime(year_start_i, month_start_i, 1, 0, 0)
    df = build_hourly_dataframe(values, start_date, value_col)
    return df, file_name, value_col


def fetch_daily_dataframe_for_code(
    code: str,
    year_start: str,
    year_end: str,
    month_start: str,
    month_end: str,
    mode_type: str,
    throttled_get,
    headers: dict,
    progress_callback=None,
):
    try:
        num, data_label, chart_title, file_suffix = build_daily_base(mode_type)
        base_url = build_daily_base_url(mode_type)
    except ValueError:
        return None, None, None, None

    first_url = build_daily_url(base_url, code, num, f"{year_start}0101", f"{year_start}1231")
    station_name = fetch_station_name(throttled_get, headers, first_url)
    if progress_callback:
        progress_callback(increment=False, station_name=station_name)

    out_dir = Path("water_info")
    out_dir.mkdir(parents=True, exist_ok=True)
    file_name = out_dir / f"{code}_{station_name}_{year_start}年{month_start}-{year_end}年{month_end}{file_suffix}"

    years = list(range(int(year_start), int(year_end) + 1))
    all_values, all_dates = [], []
    for year in years:
        url = build_daily_url(base_url, code, num, f"{year}0101", f"{year}1231")
        vals = fetch_daily_values(throttled_get, headers, url)
        last = calendar.monthrange(year, 12)[1]
        dates = pd.date_range(start=f"{year}-01-01", end=f"{year}-12-{last}", freq="D")
        n = min(len(dates), len(vals))
        all_dates += list(dates[:n])
        all_values += vals[:n]
        if progress_callback:
            progress_callback(increment=True)

    start_dt = datetime(int(year_start), int(month_start.replace("月", "")), 1)
    end_dt = datetime(
        int(year_end),
        int(month_end.replace("月", "")),
        calendar.monthrange(int(year_end), int(month_end.replace("月", "")))[1],
    )
    df = build_daily_dataframe(all_values, all_dates, data_label, start_dt, end_dt)
    return df, file_name, data_label, chart_title
