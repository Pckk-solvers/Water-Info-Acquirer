"""DataFrame builders for water_info."""

from __future__ import annotations

from datetime import datetime

import pandas as pd


def build_hourly_dataframe(values, start_date: datetime, value_col: str, *, mode_type: str):
    """時間データをDataFrame化する。

    水文水質DBの時刻値は source 上では 1:00..24:00 として並ぶため、
    internal raw timestamp は月初 00:00 ではなく月初 01:00 から始める。
    """

    observed_start = pd.Timestamp(start_date) + pd.Timedelta(hours=1)
    data_date = pd.date_range(start=observed_start, periods=len(values), freq="h")
    df = pd.DataFrame(values, index=data_date, columns=pd.Index([value_col]))
    df = df.reset_index().rename(columns={"index": "datetime"})
    if mode_type == "U":
        df["period_start_at"] = df["datetime"] - pd.to_timedelta(1, "h")
        df["period_end_at"] = df["datetime"]
        df["sheet_year"] = pd.to_datetime(df["period_end_at"], errors="coerce").dt.year
    else:
        df["sheet_year"] = pd.to_datetime(df["datetime"], errors="coerce").dt.year
    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
    return df


def build_daily_dataframe(values, dates, value_col: str, start_dt: datetime, end_dt: datetime):
    """日データをDataFrame化し、期間でフィルタリングする。"""
    df = pd.DataFrame({value_col: values}, index=dates)
    df = df[(df.index >= start_dt) & (df.index <= end_dt)]
    df.sort_index(inplace=True)
    return df
