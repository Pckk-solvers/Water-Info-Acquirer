"""Fetch flows for water_info."""

from __future__ import annotations

import calendar
from datetime import datetime
from pathlib import Path
from typing import cast

import pandas as pd

from ..infra.dataframe_utils import build_daily_dataframe, build_hourly_dataframe
from ..infra.fetching import fetch_daily_values, fetch_hourly_values, fetch_station_name
from ..infra.url_builder import build_daily_base, build_daily_base_url, build_daily_url, build_hourly_base, build_hourly_url
from ..infra.url_logger import log_urls

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


def _shift_year_month(year: int, month: int, delta_months: int) -> tuple[int, int]:
    total = year * 12 + (month - 1) + delta_months
    shifted_year, shifted_month0 = divmod(total, 12)
    return shifted_year, shifted_month0 + 1


def _iter_year_months(
    start_year: int,
    start_month: int,
    end_year: int,
    end_month: int,
) -> list[tuple[int, int]]:
    items: list[tuple[int, int]] = []
    year = start_year
    month = start_month
    while (year, month) <= (end_year, end_month):
        items.append((year, month))
        year, month = _shift_year_month(year, month, 1)
    return items


def _hourly_request_window(
    *,
    year_start: str,
    month_start: str,
    year_end: str,
    month_end: str,
) -> tuple[datetime, datetime]:
    start_month_num = int(month_start.replace("月", ""))
    end_month_num = int(month_end.replace("月", ""))
    request_start = datetime(int(year_start), start_month_num, 1, 0, 0)
    next_year, next_month = _shift_year_month(int(year_end), end_month_num, 1)
    request_end_exclusive = datetime(next_year, next_month, 1, 0, 0)
    return request_start, request_end_exclusive


def _filter_hourly_publish_window(
    df: pd.DataFrame,
    *,
    mode_type: str,
    request_start: datetime,
    request_end_exclusive: datetime,
) -> pd.DataFrame:
    if df.empty:
        return df

    work = df.copy()
    if mode_type in {"S", "R"}:
        observed = pd.to_datetime(work["datetime"], errors="coerce")
        mask = (observed >= request_start) & (observed <= request_end_exclusive)
        return work.loc[mask.fillna(False)].reset_index(drop=True)

    raw_period_end = cast(pd.Series, work["period_end_at"]) if "period_end_at" in work.columns else cast(pd.Series, work["datetime"])
    period_end = pd.to_datetime(raw_period_end, errors="coerce")
    mask = (period_end >= request_start) & (period_end <= request_end_exclusive)
    return work.loc[mask.fillna(False)].reset_index(drop=True)


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
    should_stop=None,
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

    request_start, request_end_exclusive = _hourly_request_window(
        year_start=year_start,
        month_start=month_start,
        year_end=year_end,
        month_end=month_end,
    )
    fetch_start_year, fetch_start_month = _shift_year_month(
        int(year_start),
        int(month_start.replace("月", "")),
        -1,
    )
    fetch_months = _iter_year_months(
        fetch_start_year,
        fetch_start_month,
        int(year_end),
        int(month_end.replace("月", "")),
    )
    url_month = [f"{year}{month:02d}01" for year, month in fetch_months]

    first_date = url_month[0]
    first_url = build_hourly_url(code, num, mode_str, first_date, f"{year_end}1231")
    station_name = fetch_station_name(throttled_get, headers, first_url, should_stop=should_stop)
    if progress_callback:
        progress_callback(increment=False, station_name=station_name)

    # 呼び出し側はファイル名のstemのみを参照するため、実ファイル用ディレクトリは作成しない
    file_name = Path(f"{code}_{station_name}_{year_start}年{month_start}-{year_end}年{month_end}{file_suffix}")

    url_list = [build_hourly_url(code, num, mode_str, um, f"{year_end}1231") for um in url_month]
    log_urls(
        header=f"hourly code={code} mode={mode_type} period={year_start}/{month_start}-{year_end}/{month_end}",
        urls=url_list,
    )
    values = fetch_hourly_values(
        throttled_get,
        headers,
        url_list,
        drop_last_each=False,
        on_chunk=lambda: progress_callback(increment=True) if progress_callback else None,
        should_stop=should_stop,
    )

    start_date = datetime(fetch_start_year, fetch_start_month, 1, 0, 0)
    df = build_hourly_dataframe(values, start_date, value_col, mode_type=mode_type)
    df = _filter_hourly_publish_window(
        df,
        mode_type=mode_type,
        request_start=request_start,
        request_end_exclusive=request_end_exclusive,
    )
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
    should_stop=None,
):
    try:
        num, data_label, chart_title, file_suffix = build_daily_base(mode_type)
        base_url = build_daily_base_url(mode_type)
    except ValueError:
        return None, None, None, None

    first_url = build_daily_url(base_url, code, num, f"{year_start}0101", f"{year_start}1231")
    station_name = fetch_station_name(throttled_get, headers, first_url, should_stop=should_stop)
    if progress_callback:
        progress_callback(increment=False, station_name=station_name)

    # 呼び出し側はファイル名のstemのみを参照するため、実ファイル用ディレクトリは作成しない
    file_name = Path(f"{code}_{station_name}_{year_start}年{month_start}-{year_end}年{month_end}{file_suffix}")

    years = list(range(int(year_start), int(year_end) + 1))
    all_values, all_dates = [], []
    daily_urls: list[str] = []
    for year in years:
        url = build_daily_url(base_url, code, num, f"{year}0101", f"{year}1231")
        daily_urls.append(url)
        vals = list(cast(list[float | str], fetch_daily_values(throttled_get, headers, url, should_stop=should_stop)))
        last = calendar.monthrange(year, 12)[1]
        dates = pd.date_range(start=f"{year}-01-01", end=f"{year}-12-{last}", freq="D")
        n = min(len(dates), len(vals))
        all_dates += list(dates[:n])
        all_values += vals[:n]
        if progress_callback:
            progress_callback(increment=True)
    log_urls(
        header=f"daily code={code} mode={mode_type} period={year_start}/{month_start}-{year_end}/{month_end}",
        urls=daily_urls,
    )

    start_dt = datetime(int(year_start), int(month_start.replace("月", "")), 1)
    end_dt = datetime(
        int(year_end),
        int(month_end.replace("月", "")),
        calendar.monthrange(int(year_end), int(month_end.replace("月", "")))[1],
    )
    df = build_daily_dataframe(all_values, all_dates, data_label, start_dt, end_dt)
    return df, file_name, data_label, chart_title
