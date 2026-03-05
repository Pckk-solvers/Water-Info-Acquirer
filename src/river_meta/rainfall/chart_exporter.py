"""降雨グラフ（2軸複合チャート）をPNG画像として出力する。

年最大雨量の発生日時を中心に、時刻雨量（棒グラフ・左軸）と
累加雨量（折れ線グラフ・右軸）を1枚のPNGに描画する。
指標ごとに切り取り範囲・X軸ラベル間隔を最適化する。
"""

from __future__ import annotations

import math
from datetime import timedelta
from pathlib import Path
from typing import Any, Callable

import matplotlib
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

matplotlib.use("Agg")  # GUIバックエンドを使わない

# 日本語フォント設定 (Windows: MS Gothic / Yu Gothic, macOS: Hiragino, Linux: IPAGothic)
import platform as _platform

_os_name = _platform.system()
if _os_name == "Windows":
    matplotlib.rcParams["font.family"] = ["MS Gothic", "Yu Gothic", "Meiryo", "sans-serif"]
elif _os_name == "Darwin":
    matplotlib.rcParams["font.family"] = ["Hiragino Sans", "Hiragino Kaku Gothic Pro", "sans-serif"]
else:
    matplotlib.rcParams["font.family"] = ["IPAGothic", "IPAPGothic", "Noto Sans CJK JP", "sans-serif"]
matplotlib.rcParams["axes.unicode_minus"] = False

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
    should_stop: Callable[[], bool] | None = None,
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

    # 観測所ごとのサブディレクトリ
    safe_station_name = str(station_name).replace("/", "_").replace("\\", "_") if station_name else ""
    safe_station_key = str(station_key).replace("/", "_").replace("\\", "_")
    subdir_name = f"{safe_station_name}_{safe_station_key}" if safe_station_name else safe_station_key
    out_dir = Path(output_dir) / subdir_name
    out_dir.mkdir(parents=True, exist_ok=True)

    # 時系列を datetime インデックスにしておく
    ts = timeseries_df.copy()
    ts["観測時刻"] = pd.to_datetime(ts["観測時刻"], errors="coerce")
    ts = ts.dropna(subset=["観測時刻"]).set_index("観測時刻").sort_index()

    paths: list[Path] = []

    for _, row in annual_max_df.iterrows():
        if should_stop is not None:
            try:
                if should_stop():
                    break
            except Exception:
                pass
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

        # 品質情報を追加
        if "品質" in ts.columns:
            window["品質"] = ts["品質"].reindex(window.index).fillna("正常")
        else:
            window["品質"] = "正常"

        # ファイル名（サブディレクトリ内なので station_key プレフィックス不要）
        safe_name = metric_raw.replace("/", "_")
        filename = f"{year}_{safe_name}.png"
        output_path = out_dir / filename

        title_metric = metric_raw.replace("雨量", "最大雨量")
        title = f"{station_name} {year}年 {title_metric}" if station_name else f"{station_key} {year}年 {title_metric}"

        if should_stop is not None:
            try:
                if should_stop():
                    break
            except Exception:
                pass

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
_BAR_COLOR = "#2E6EB5"
_LINE_COLOR = "#E74C3C"
_PEAK_LINE_COLOR = "#2ECC71"
_MISSING_COLOR = "#CCCCCC"


def _nice_step(max_value: float, n_ticks: int) -> float:
    """max_value を n_ticks 分割するきりのいいステップ値を返す。

    例: max_value=370, n_ticks=5 → step=100 (0, 100, 200, 300, 400)
    """
    if max_value <= 0 or n_ticks <= 1:
        return max(max_value, 1.0)
    raw = max_value / (n_ticks - 1)
    import math
    magnitude = 10 ** math.floor(math.log10(raw)) if raw > 0 else 1
    normalized = raw / magnitude
    if normalized <= 1:
        nice = 1
    elif normalized <= 2:
        nice = 2
    elif normalized <= 5:
        nice = 5
    else:
        nice = 10
    return nice * magnitude


def _hour_label(dt: pd.Timestamp) -> str:
    """datetime の hour を 1〜24 表記に変換する。

    1時間累加雨量の慣例に従い、hour+1 で表示する。
    0時 → 1、23時 → 24。
    """
    return f"{dt.hour + 1}"


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
    import numpy as np

    fig, ax1 = plt.subplots(figsize=(14, 6))

    times = window.index
    rainfall = window[rainfall_col].values
    cumulative = window["累加雨量(mm)"].values

    # --- 欠測帯の描画 (背景灰色帯) ---
    if "品質" in window.columns:
        half_h = timedelta(minutes=30)
        missing_drawn = False
        for dt_idx in times:
            if str(window.at[dt_idx, "品質"]) == "欠測":
                ax1.axvspan(
                    dt_idx - half_h, dt_idx + half_h,
                    color=_MISSING_COLOR, alpha=0.3, zorder=0,
                    label="欠測" if not missing_drawn else None,
                )
                missing_drawn = True

    # --- 左軸: 時刻雨量 (棒グラフ・隙間なし) ---
    bar_width = timedelta(hours=1)
    ax1.bar(times, rainfall, width=bar_width, color=_BAR_COLOR, edgecolor="black", linewidth=0.5, alpha=0.85, label="時間雨量", zorder=2)
    ax1.set_ylabel("時間雨量 (mm)", color=_BAR_COLOR, fontsize=11)
    ax1.tick_params(axis="y", labelcolor=_BAR_COLOR)

    # Y軸の上限を少し余裕をもたせる
    max_rainfall = float(max(rainfall)) if len(rainfall) > 0 else 1.0

    # --- 右軸: 累加雨量 (折れ線グラフ) ---
    ax2 = ax1.twinx()
    ax2.plot(times, cumulative, color=_LINE_COLOR, linewidth=2.0, label="累加雨量", zorder=3)
    ax2.set_ylabel("累加雨量 (mm)", color=_LINE_COLOR, fontsize=11)
    ax2.tick_params(axis="y", labelcolor=_LINE_COLOR)

    max_cumulative = float(max(cumulative)) if len(cumulative) > 0 else 1.0

    # --- 両軸の目盛を手動設定して揃える ---
    n_divisions = 6  # 0を含めて7目盛
    left_step = _nice_step(max_rainfall * 1.3, n_divisions + 1)
    right_step = _nice_step(max_cumulative * 1.1, n_divisions + 1)
    left_ticks = [left_step * i for i in range(n_divisions + 1)]
    right_ticks = [right_step * i for i in range(n_divisions + 1)]
    ax1.set_yticks(left_ticks)
    ax1.set_ylim(0, left_ticks[-1])
    ax2.set_yticks(right_ticks)
    ax2.set_ylim(0, right_ticks[-1])

    # --- ピーク時刻マーカー ---
    ax1.axvline(x=event_dt, color=_PEAK_LINE_COLOR, linestyle="--", linewidth=1.5, label="ピーク時刻", zorder=4)

    # --- X軸設定: 1〜24時表記、回転なし ---
    ax1.xaxis.set_major_locator(mdates.HourLocator(interval=label_interval))
    ax1.xaxis.set_major_formatter(
        plt.FuncFormatter(lambda x, _: _hour_label(mdates.num2date(x)))  # type: ignore[arg-type]
    )
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=0, ha="center", fontsize=8)
    ax1.set_xlabel("時刻", fontsize=11)

    # --- 日付境界線 (24時=23:00 と 1時=00:00 の境界 = 23:30 位置) ---
    window_start = times.min()
    window_end = times.max()
    first_midnight = (window_start + timedelta(days=1)).normalize()
    dt = first_midnight
    boundary_offset = timedelta(minutes=-30)  # 00:00 の30分前 = 23:30
    while dt <= window_end:
        boundary = dt + boundary_offset
        ax1.axvline(x=boundary, color="black", linewidth=1.0, zorder=1)
        ax1.text(
            boundary, ax1.get_ylim()[1] * 0.97,
            dt.strftime("%m/%d"),
            ha="left", va="top", fontsize=8, fontweight="bold",
            bbox={"facecolor": "white", "alpha": 0.7, "edgecolor": "none", "pad": 1},
            zorder=5,
        )
        dt += timedelta(days=1)

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
