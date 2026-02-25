"""降雨グラフ（2軸複合チャート）をPNG画像として出力する。

年最大雨量の発生日時を中心に、時刻雨量（棒グラフ・左軸）と
累加雨量（折れ線グラフ・右軸）を1枚のPNGに描画する。
指標ごとに切り取り範囲・X軸ラベル間隔を最適化する。
"""

from __future__ import annotations

import math
from datetime import timedelta
from pathlib import Path
from typing import Any

import matplotlib
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

matplotlib.use("Agg")  # GUIバックエンドを使わない

# ---------------------------------------------------------------------------
# 指標定義
# ---------------------------------------------------------------------------

METRICS = [
    "1時間雨量",
    "3時間雨量",
    "6時間雨量",
    "12時間雨量",
    "24時間雨量",
    "48時間雨量",
]

_METRIC_HOURS: dict[str, int] = {
    "1時間雨量": 1,
    "3時間雨量": 3,
    "6時間雨量": 6,
    "12時間雨量": 12,
    "24時間雨量": 24,
    "48時間雨量": 48,
}

# 合計表示時間 (hours)
_TOTAL_HOURS: dict[str, int] = {
    "1時間雨量": 24,
    "3時間雨量": 48,
    "6時間雨量": 48,
    "12時間雨量": 72,
    "24時間雨量": 72,
    "48時間雨量": 96,
}

# X軸ラベル間隔 (hours)
_LABEL_INTERVAL: dict[str, int] = {
    "1時間雨量": 1,
    "3時間雨量": 3,
    "6時間雨量": 3,
    "12時間雨量": 3,
    "24時間雨量": 3,
    "48時間雨量": 6,
}


# ---------------------------------------------------------------------------
# 比率算出ユーティリティ
# ---------------------------------------------------------------------------


def _compute_before_after(metric: str) -> tuple[timedelta, timedelta]:
    """指標の時間スケールに応じた前後の切り取り幅を返す。

    before:after 比率 = 1 + 2 × (metric_hours - 1) / 47
        - 1時間雨量 → 1:1 (均等)
        - 48時間雨量 → 3:1 (前を重視)
    """
    metric_hours = _METRIC_HOURS[metric]
    total_hours = _TOTAL_HOURS[metric]
    ratio = 1.0 + 2.0 * (metric_hours - 1) / 47.0
    before_hours = total_hours * ratio / (ratio + 1.0)
    after_hours = total_hours - before_hours
    # 整数時間に丸める
    before_hours = round(before_hours)
    after_hours = total_hours - before_hours
    return timedelta(hours=before_hours), timedelta(hours=after_hours)


def compute_chart_config(metric: str) -> dict[str, Any]:
    """指標ごとのチャート設定を算出して辞書で返す。"""
    before, after = _compute_before_after(metric)
    return {
        "metric": metric,
        "before": before,
        "after": after,
        "total_hours": _TOTAL_HOURS[metric],
        "label_interval": _LABEL_INTERVAL[metric],
    }


# ---------------------------------------------------------------------------
# メインエントリポイント
# ---------------------------------------------------------------------------


def export_rainfall_charts(
    timeseries_df: pd.DataFrame,
    annual_max_df: pd.DataFrame,
    *,
    output_dir: str,
    station_key: str,
    station_name: str = "",
) -> list[Path]:
    """年最大雨量イベントごとにPNGチャートを生成する。

    Parameters
    ----------
    timeseries_df : pd.DataFrame
        ``build_hourly_timeseries_dataframe`` の出力。
    annual_max_df : pd.DataFrame
        ``build_annual_max_dataframe`` の出力。
    output_dir : str
        PNG出力先ディレクトリ。
    station_key : str
        観測所キー (ファイル名に使用)。
    station_name : str
        観測所名 (タイトルに使用)。

    Returns
    -------
    list[Path]
        生成したPNGファイルのパスリスト。
    """
    if timeseries_df is None or timeseries_df.empty:
        return []
    if annual_max_df is None or annual_max_df.empty:
        return []

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    # 時系列を datetime インデックスにしておく
    ts = timeseries_df.copy()
    ts["観測時刻"] = pd.to_datetime(ts["観測時刻"], errors="coerce")
    ts = ts.dropna(subset=["観測時刻"]).set_index("観測時刻").sort_index()

    paths: list[Path] = []

    for _, row in annual_max_df.iterrows():
        metric_raw = str(row.get("指標", ""))
        if metric_raw not in _METRIC_HOURS:
            continue

        event_dt = pd.to_datetime(row.get("発生日時"), errors="coerce")
        if pd.isna(event_dt):
            continue

        year = int(row["年"])
        config = compute_chart_config(metric_raw)

        start = event_dt - config["before"]
        end = event_dt + config["after"]
        window = ts.loc[start:end].copy()

        if window.empty:
            continue

        # 1時間雨量を数値化
        rainfall_col = "1時間雨量(mm)"
        if rainfall_col not in window.columns:
            continue
        window[rainfall_col] = pd.to_numeric(window[rainfall_col], errors="coerce").fillna(0.0)

        # 累加雨量 (切り取り範囲先頭からの累積)
        window["累加雨量(mm)"] = window[rainfall_col].cumsum()

        # ファイル名
        safe_name = metric_raw.replace("/", "_")
        filename = f"{station_key}_{year}_{safe_name}.png"
        output_path = out_dir / filename

        title = f"{station_name} {year}年 {metric_raw} 年最大雨量" if station_name else f"{station_key} {year}年 {metric_raw} 年最大雨量"

        _render_chart(
            window=window,
            rainfall_col=rainfall_col,
            event_dt=event_dt,
            title=title,
            label_interval=config["label_interval"],
            output_path=output_path,
        )
        paths.append(output_path)

    return paths


# ---------------------------------------------------------------------------
# 描画
# ---------------------------------------------------------------------------

# カラー定義
_BAR_COLOR = "#4A90D9"
_LINE_COLOR = "#E74C3C"
_PEAK_LINE_COLOR = "#2ECC71"


def _render_chart(
    *,
    window: pd.DataFrame,
    rainfall_col: str,
    event_dt: pd.Timestamp,
    title: str,
    label_interval: int,
    output_path: Path,
) -> None:
    """2軸複合チャートを描画してPNG保存する。"""
    fig, ax1 = plt.subplots(figsize=(14, 6))

    times = window.index
    rainfall = window[rainfall_col].values
    cumulative = window["累加雨量(mm)"].values

    # --- 左軸: 時刻雨量 (棒グラフ) ---
    bar_width = timedelta(hours=0.8)
    ax1.bar(times, rainfall, width=bar_width, color=_BAR_COLOR, alpha=0.7, label="時間雨量", zorder=2)
    ax1.set_xlabel("観測時刻", fontsize=11)
    ax1.set_ylabel("時間雨量 (mm)", color=_BAR_COLOR, fontsize=11)
    ax1.tick_params(axis="y", labelcolor=_BAR_COLOR)

    # Y軸の上限を少し余裕をもたせる
    max_rainfall = max(rainfall) if len(rainfall) > 0 else 1
    ax1.set_ylim(0, max_rainfall * 1.3 if max_rainfall > 0 else 1)

    # --- 右軸: 累加雨量 (折れ線グラフ) ---
    ax2 = ax1.twinx()
    ax2.plot(times, cumulative, color=_LINE_COLOR, linewidth=2.0, label="累加雨量", zorder=3)
    ax2.set_ylabel("累加雨量 (mm)", color=_LINE_COLOR, fontsize=11)
    ax2.tick_params(axis="y", labelcolor=_LINE_COLOR)

    max_cumulative = max(cumulative) if len(cumulative) > 0 else 1
    ax2.set_ylim(0, max_cumulative * 1.1 if max_cumulative > 0 else 1)

    # --- ピーク時刻マーカー ---
    ax1.axvline(x=event_dt, color=_PEAK_LINE_COLOR, linestyle="--", linewidth=1.5, label="ピーク時刻", zorder=4)

    # --- X軸設定 ---
    ax1.xaxis.set_major_locator(mdates.HourLocator(interval=label_interval))
    ax1.xaxis.set_major_formatter(mdates.DateFormatter("%m/%d\n%H:%M"))
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha="right", fontsize=8)

    # --- タイトル・凡例 ---
    ax1.set_title(title, fontsize=14, fontweight="bold", pad=12)

    # 凡例を統合
    handles1, labels1 = ax1.get_legend_handles_labels()
    handles2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(handles1 + handles2, labels1 + labels2, loc="upper left", fontsize=9)

    ax1.grid(axis="y", alpha=0.3)
    fig.tight_layout()

    fig.savefig(str(output_path), dpi=150, bbox_inches="tight")
    plt.close(fig)
