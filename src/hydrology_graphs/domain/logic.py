"""ドメインの純粋ロジック。

入力データに対する判定、窓切り出し、年最大計算など、
副作用を持たない処理をここに集約する。
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any, cast

import pandas as pd

from .constants import ANNUAL_GRAPH_TYPES, EVENT_GRAPH_TYPES, GRAPH_REQUIREMENTS, GRAPH_TYPES
from .models import GraphTarget


def _time_column(df: pd.DataFrame) -> str:
    if "period_end_at" in df.columns:
        period_end = pd.to_datetime(df["period_end_at"], errors="coerce")
        if not period_end.isna().all():
            return "period_end_at"
    return "observed_at"


def ensure_graph_type_supported(graph_type: str) -> bool:
    """指定されたグラフ種別がアプリで扱えるかを返す。"""

    return graph_type in GRAPH_TYPES


def graph_category(graph_type: str) -> str:
    """グラフ種別を event / annual に分類する。"""

    if graph_type in EVENT_GRAPH_TYPES:
        return "event"
    if graph_type in ANNUAL_GRAPH_TYPES:
        return "annual"
    raise ValueError(f"unknown graph_type: {graph_type}")


def required_metric_interval(graph_type: str) -> tuple[str, str]:
    """グラフ種別から必要な metric と interval を返す。"""

    requirement = GRAPH_REQUIREMENTS[graph_type]
    return requirement.metric, requirement.interval


def is_event_graph(graph_type: str) -> bool:
    """イベント系グラフかどうかを返す。"""

    return GRAPH_REQUIREMENTS[graph_type].event_graph


def event_window_bounds(base_date: date, window_days: int) -> tuple[datetime, datetime]:
    """3日または5日のイベント窓の開始・終了を返す。"""

    if window_days not in (3, 5):
        raise ValueError("window_days must be 3 or 5")
    side_days = (window_days - 1) // 2
    start = datetime.combine(base_date - timedelta(days=side_days), datetime.min.time())
    end = start + timedelta(days=window_days)
    return start, end


def event_capture_window_bounds(base_date: date, window_days: int, terminal_padding_hours: int = 0) -> tuple[datetime, datetime]:
    """描画・検証で使う capture window の開始・終了を返す。"""

    start, end = event_window_bounds(base_date, window_days)
    if terminal_padding_hours < 0:
        raise ValueError("terminal_padding_hours must be >= 0")
    return start, end + timedelta(hours=terminal_padding_hours)


def expected_event_index(
    base_date: date,
    window_days: int,
    *,
    terminal_padding_hours: int = 0,
) -> pd.DatetimeIndex:
    """イベント窓に含まれる想定時刻の索引を作る。"""

    start, end = event_capture_window_bounds(base_date, window_days, terminal_padding_hours)
    return pd.date_range(start=start, end=end - timedelta(hours=1), freq="1h")


def extract_event_series(
    df: pd.DataFrame,
    base_date: date,
    window_days: int,
    *,
    terminal_padding_hours: int = 0,
) -> pd.DataFrame:
    """対象期間のデータだけを切り出し、同一時刻の重複は後勝ちでまとめる。"""

    start, end = event_capture_window_bounds(base_date, window_days, terminal_padding_hours)
    work = df.copy()
    tcol = _time_column(work)
    work[tcol] = pd.to_datetime(work[tcol], errors="coerce")
    work = work.dropna(subset=[tcol]).sort_values(tcol)
    mask = (work[tcol] >= start) & (work[tcol] < end)
    sliced = work.loc[mask].copy()
    if sliced.empty:
        return sliced
    return sliced.drop_duplicates(subset=[tcol], keep="last").reset_index(drop=True)


def validate_event_series_complete(
    df: pd.DataFrame,
    base_date: date,
    window_days: int,
    *,
    terminal_padding_hours: int = 0,
) -> tuple[bool, str | None]:
    """イベント窓に欠損がないかを確認する。"""

    expected = expected_event_index(base_date, window_days, terminal_padding_hours=terminal_padding_hours)
    if df.empty:
        return False, "対象期間のデータが存在しません。"
    tcol = _time_column(df)
    if tcol not in df.columns:
        return False, f"{tcol} 列が見つかりません。"
    work = df.copy()
    work[tcol] = pd.to_datetime(work[tcol], errors="coerce")
    work = work.dropna(subset=[tcol]).set_index(tcol).reindex(expected)
    if "value" not in work.columns:
        return False, "value 列が見つかりません。"
    if bool(cast(pd.Series, work["value"]).isna().any()):
        return False, "対象期間内に欠損値があります。"
    if "quality" in work.columns and bool((work["quality"] == "missing").any()):
        return False, "対象期間内に quality=missing が含まれます。"
    return True, None


def annual_max_series(df: pd.DataFrame) -> pd.Series:
    """年ごとの最大値系列を返す。"""

    if df.empty:
        return pd.Series(dtype="float64")
    work = df.copy()
    tcol = _time_column(work)
    work[tcol] = pd.to_datetime(work[tcol], errors="coerce")
    work["value"] = pd.to_numeric(work["value"], errors="coerce")
    work = work.dropna(subset=[tcol, "value"])
    if work.empty:
        return pd.Series(dtype="float64")
    work["year"] = work[tcol].dt.year
    return cast(pd.Series, work.groupby("year")["value"].max().sort_index())


def annual_max_by_year(df: pd.DataFrame) -> pd.DataFrame:
    """年最大値と、その最大値が出た観測時刻を返す。"""

    if df.empty:
        return pd.DataFrame(columns=pd.Index(["year", "period_end_at", "value"]))
    work = df.copy()
    tcol = _time_column(work)
    work[tcol] = pd.to_datetime(work[tcol], errors="coerce")
    work["value"] = pd.to_numeric(work["value"], errors="coerce")
    work = work.dropna(subset=[tcol, "value"])
    if work.empty:
        return pd.DataFrame(columns=pd.Index(["year", "period_end_at", "value"]))
    work["year"] = work[tcol].dt.year
    rows: list[dict[str, object]] = []
    for year, group in work.groupby("year", sort=True):
        idx = cast(int, cast(pd.Series, group["value"]).astype(float).idxmax())
        row = work.loc[idx]
        rows.append(
            {
                "year": int(cast(Any, year)),
                "period_end_at": row[tcol],
                "value": float(row["value"]),
            }
        )
    return pd.DataFrame(rows)


def has_min_years(series: pd.Series, min_years: int = 10) -> bool:
    """年最大グラフに必要な年数を満たすかを確認する。"""

    return int(series.shape[0]) >= min_years


def threshold_key(source: str, station_key: str, graph_type: str) -> str:
    """基準線検索に使う結合キーを作る。"""

    return f"{source}|{station_key}|{graph_type}"


def build_output_target(target: GraphTarget) -> str:
    """描画対象の出力名に使う文字列を返す。"""

    return target.target_id
