"""DataFrame builders for water_info."""

from __future__ import annotations

from datetime import datetime
import pandas as pd


def build_hourly_dataframe(values, start_date: datetime, value_col: str):
    """時間データをDataFrame化し、display_dt と sheet_year を付与する。"""
    data_date = pd.date_range(start=start_date, periods=len(values), freq="h")
    df = pd.DataFrame(values, index=data_date, columns=[value_col])
    df = df.reset_index().rename(columns={"index": "datetime"})
    df["display_dt"] = df["datetime"] + pd.to_timedelta(1, "h")
    df["sheet_year"] = df["datetime"].dt.year
    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
    return df


def build_daily_dataframe(values, dates, value_col: str, start_dt: datetime, end_dt: datetime):
    """日データをDataFrame化し、期間でフィルタリングする。"""
    df = pd.DataFrame({value_col: values}, index=dates)
    df = df[(df.index >= start_dt) & (df.index <= end_dt)]
    df.sort_index(inplace=True)
    return df
