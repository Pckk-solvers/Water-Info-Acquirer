"""水文グラフの Matplotlib 描画処理。

このモジュールは、グラフ種別ごとの見た目と基準線の重畳を担当する。
"""

from __future__ import annotations

from datetime import timedelta
from io import BytesIO
from typing import Any, cast
import warnings

import matplotlib

matplotlib.use("Agg")

import japanize_matplotlib  # noqa: F401  # Matplotlib の日本語フォント設定を有効化する
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd

from ..domain.constants import (
    GRAPH_ANNUAL_DISCHARGE,
    GRAPH_ANNUAL_RAINFALL,
    GRAPH_ANNUAL_WATER_LEVEL,
    GRAPH_HYDRO_DISCHARGE,
    GRAPH_HYDRO_WATER_LEVEL,
    GRAPH_HYETOGRAPH,
)
from ..domain.logic import annual_max_by_year
from ..domain.models import ThresholdRecord

_LINESTYLE_MAP = {"solid": "-", "dashed": "--", "dotted": ":", "dashdot": "-."}

warnings.filterwarnings(
    "ignore",
    message=r".*distutils Version classes are deprecated.*",
    category=DeprecationWarning,
    module=r"japanize_matplotlib(\..*)?$",
)
warnings.filterwarnings(
    "ignore",
    message=r".*distutils Version classes are deprecated.*",
    category=DeprecationWarning,
    module=r"setuptools\._distutils\.version",
)
warnings.filterwarnings(
    "ignore",
    message=r".*distutils Version classes are deprecated.*",
    category=DeprecationWarning,
)


def render_graph_png(
    *,
    graph_type: str,
    station_name: str,
    df: pd.DataFrame,
    graph_style: dict[str, Any],
    thresholds: list[ThresholdRecord],
    time_display_mode: str = "datetime",
) -> bytes:
    """描画対象とスタイルから PNG バイト列を生成する。"""

    figure_width = float(graph_style.get("figure_width", 12))
    figure_height = float(graph_style.get("figure_height", 6))
    dpi = int(graph_style.get("dpi", 120))
    fig, ax = plt.subplots(
        figsize=(figure_width, figure_height),
        dpi=dpi,
    )
    fig.patch.set_facecolor(graph_style.get("background_color", "#FFFFFF"))
    ax.set_facecolor(graph_style.get("background_color", "#FFFFFF"))
    _apply_common_axes_style(ax, graph_style)

    # グラフ種別ごとに描画方法を切り替える。イベント系は折れ線/棒、年最大系は年次棒。
    if graph_type == GRAPH_HYETOGRAPH:
        _plot_hyetograph(ax, df, graph_style)
    elif graph_type in (GRAPH_HYDRO_DISCHARGE, GRAPH_HYDRO_WATER_LEVEL):
        _plot_hydro(ax, df, graph_style)
    elif graph_type in (GRAPH_ANNUAL_RAINFALL, GRAPH_ANNUAL_DISCHARGE, GRAPH_ANNUAL_WATER_LEVEL):
        _plot_annual(ax, df, graph_style)
    else:
        raise ValueError(f"Unsupported graph_type: {graph_type}")

    # 基準線は最後に重ねることで、主系列の見た目を邪魔しにくくする。
    _plot_thresholds(ax, thresholds, graph_style)
    _plot_date_boundaries(ax, df, graph_style, graph_type=graph_type)
    _apply_title_axis_labels(ax, graph_style, station_name)
    _apply_axis_details(ax, graph_style, time_display_mode=time_display_mode)

    legend_cfg = graph_style.get("legend", {})
    if legend_cfg.get("enabled", True):
        loc = str(legend_cfg.get("position", "upper right"))
        anchor = legend_cfg.get("fixed_anchor")
        handles, labels = ax.get_legend_handles_labels()
        filtered = [(h, label) for h, label in zip(handles, labels, strict=False) if _legend_label_visible(label)]
        handles = [h for h, _ in filtered]
        labels = [label for _, label in filtered]
        # 位置を固定したい場合は bbox_to_anchor を使い、通常は標準位置を使う。
        if isinstance(anchor, dict) and "x" in anchor and "y" in anchor:
            if handles:
                ax.legend(handles, labels, loc=loc, bbox_to_anchor=(float(anchor["x"]), float(anchor["y"])))
        elif handles:
            ax.legend(handles, labels, loc=loc)

    margin = graph_style.get("margin", {})
    fig.subplots_adjust(
        left=float(margin.get("left", 0.08)),
        right=1.0 - float(margin.get("right", 0.04)),
        top=1.0 - float(margin.get("top", 0.08)),
        bottom=float(margin.get("bottom", 0.12)),
    )

    buf = BytesIO()
    fig.savefig(
        buf,
        format="png",
        transparent=bool(graph_style.get("export", {}).get("transparent_background", False)),
    )
    plt.close(fig)
    return buf.getvalue()


def _apply_common_axes_style(ax, graph_style: dict[str, Any]) -> None:
    """全グラフ共通の軸スタイルを当てる。"""

    font = graph_style.get("font", {})
    tick_size = float(font.get("tick_size", graph_style.get("font_size", 11)))
    ax.tick_params(labelsize=tick_size)
    grid = graph_style.get("grid", {})
    if grid.get("enabled", True):
        ax.grid(
            True,
            linestyle=str(grid.get("style", "--")),
            color=str(grid.get("color", "#CBD5E1")),
            alpha=float(grid.get("alpha", 0.7)),
        )
    if graph_style.get("invert_y_axis"):
        ax.invert_yaxis()
        ax.xaxis.tick_top()


def _plot_hyetograph(ax, df: pd.DataFrame, graph_style: dict[str, Any]) -> None:
    """ハイエトグラフを描く。"""

    time_col = _time_column_for_plot(df)
    data = df.sort_values(time_col).copy()
    data[time_col] = pd.to_datetime(data[time_col], errors="coerce")
    numeric_values = cast(pd.Series, pd.to_numeric(data["value"], errors="coerce"))
    data["value"] = numeric_values.fillna(0.0)
    bar_cfg = graph_style.get("bar", {})
    width_hours = float(graph_style.get("x_axis", {}).get("tick_interval_hours", 1))
    width = float(bar_cfg.get("width", 0.8)) / max(width_hours, 1.0)
    ax.bar(
        data[time_col],
        data["value"],
        width=width,
        color=graph_style.get("bar_color", "#60A5FA"),
        label="時間雨量",
        zorder=float(graph_style.get("series", {}).get("zorder", 2)),
    )


def _plot_hydro(ax, df: pd.DataFrame, graph_style: dict[str, Any]) -> None:
    """流量・水位の折れ線グラフを描く。"""

    time_col = _time_column_for_plot(df)
    data = df.sort_values(time_col).copy()
    data[time_col] = pd.to_datetime(data[time_col], errors="coerce")
    ax.plot(
        data[time_col],
        pd.to_numeric(data["value"], errors="coerce"),
        color=graph_style.get("series_color", "#0F766E"),
        linewidth=float(graph_style.get("series_width", 1.5)),
        linestyle=_line_style(graph_style.get("series_style", "solid")),
        marker="o" if graph_style.get("show_markers", False) else None,
        label="観測値",
        zorder=float(graph_style.get("series", {}).get("zorder", 2)),
    )


def _plot_annual(ax, df: pd.DataFrame, graph_style: dict[str, Any]) -> None:
    """年最大系の棒グラフを描く。"""

    annual = annual_max_by_year(df) if "year" not in df.columns else df.copy()
    xs = [str(int(year)) for year in annual["year"].tolist()]
    ys = annual["value"].tolist()
    ax.bar(
        xs,
        ys,
        color=graph_style.get("bar_color", graph_style.get("series_color", "#1D4ED8")),
        width=float(graph_style.get("bar", {}).get("width", 0.8)),
        label="年最大値",
        zorder=float(graph_style.get("series", {}).get("zorder", 2)),
    )


def _plot_thresholds(ax, thresholds: list[ThresholdRecord], graph_style: dict[str, Any]) -> None:
    """基準線を重ねて描画する。"""

    if not thresholds:
        return
    t_cfg = graph_style.get("threshold", {})
    label_enabled = bool(t_cfg.get("label_enabled", True))
    label_offset = float(t_cfg.get("label_offset", 0.02))
    label_size = float(t_cfg.get("label_font_size", 10))
    zorder = float(t_cfg.get("zorder", 3))
    for line in thresholds:
        # 線ごとに属性が違うので 1 本ずつ描画する。
        color = line.line_color or "#DC2626"
        linestyle = _line_style(line.line_style)
        legend_label = _coerce_text(line.label) or _coerce_text(line.line_name)
        ax.axhline(
            y=line.value,
            color=color,
            linestyle=linestyle,
            linewidth=float(line.line_width or 1.2),
            zorder=zorder,
            label=legend_label or "_nolegend_",
        )
        inline_label = _coerce_text(line.label)
        if label_enabled and inline_label:
            # ラベルは左寄せで重ならない位置に置く。
            ymax = ax.get_ylim()[1]
            offset = (abs(ymax) if ymax != 0 else 1.0) * label_offset
            ax.text(
                0.01,
                line.value + offset,
                inline_label,
                transform=ax.get_yaxis_transform(),
                color=color,
                fontsize=label_size,
            )


def _plot_date_boundaries(ax, df: pd.DataFrame, graph_style: dict[str, Any], *, graph_type: str) -> None:
    """日付境界線を描画する。"""

    if graph_type not in {GRAPH_HYETOGRAPH, GRAPH_HYDRO_DISCHARGE, GRAPH_HYDRO_WATER_LEVEL}:
        return
    x_axis = graph_style.get("x_axis", {})
    if not bool(x_axis.get("date_boundary_line_enabled", False)):
        return
    try:
        offset_hours = float(x_axis.get("date_boundary_line_offset_hours", 0.0))
    except Exception:  # noqa: BLE001
        offset_hours = 0.0
    time_col = _time_column_for_plot(df)
    observed = pd.to_datetime(df[time_col], errors="coerce").dropna()
    if observed.empty:
        return
    start = observed.min().floor("D")
    end = observed.max().ceil("D")
    boundaries = pd.date_range(start=start, end=end, freq="D")
    offset = pd.Timedelta(hours=offset_hours)
    for ts in boundaries:
        boundary = ts + offset
        if observed.min() < boundary < observed.max():
            ax.axvline(
                boundary,
                color="#94A3B8",
                linestyle=":",
                linewidth=0.9,
                alpha=0.9,
                zorder=1.5,
            )


def _apply_title_axis_labels(ax, graph_style: dict[str, Any], station_name: str) -> None:
    """タイトルと軸ラベルを反映する。"""

    title_cfg = graph_style.get("title", {})
    title_tpl = _coerce_text(title_cfg.get("template", "{station_name}"))
    if title_tpl:
        title_text = _coerce_text(title_tpl.format(station_name=station_name or ""))
        if title_text:
            ax.set_title(title_text)
    axis = graph_style.get("axis", {})
    x_label = _coerce_text(axis.get("x_label", ""))
    y_label = _coerce_text(axis.get("y_label", ""))
    if x_label:
        ax.set_xlabel(x_label)
    if y_label:
        ax.set_ylabel(y_label)


def _apply_axis_details(ax, graph_style: dict[str, Any], *, time_display_mode: str = "datetime") -> None:
    """日時目盛りや数値フォーマットなどの細部を調整する。"""

    x_axis = graph_style.get("x_axis", {})
    y_axis = graph_style.get("y_axis", {})
    if x_axis.get("tick_interval_hours"):
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=max(1, int(float(x_axis["tick_interval_hours"])))))
    if _is_24h_time_display_mode(time_display_mode):
        ax.xaxis.set_major_formatter(mticker.FuncFormatter(_format_24h_tick))
    elif x_axis.get("date_format"):
        ax.xaxis.set_major_formatter(mdates.DateFormatter(str(x_axis["date_format"])))
    rotation = float(x_axis.get("tick_rotation", 0))
    align = str(x_axis.get("label_align", "center"))
    for tick in ax.get_xticklabels():
        tick.set_rotation(rotation)
        tick.set_horizontalalignment(align)
    try:
        x_margin_rate = float(x_axis.get("range_margin_rate", 0))
    except Exception:  # noqa: BLE001
        x_margin_rate = 0.0
    if x_margin_rate >= 0:
        ax.margins(x=x_margin_rate)

    y_min = y_axis.get("min")
    y_max = y_axis.get("max")
    if y_min is not None or y_max is not None:
        ax.set_ylim(bottom=y_min, top=y_max)

    tick_step = y_axis.get("tick_step")
    if tick_step is not None:
        tick_step = float(tick_step)
        low, high = ax.get_ylim()
        if tick_step > 0 and high > low:
            ticks: list[float] = []
            value = low
            while value <= high + tick_step:
                ticks.append(value)
                value += tick_step
            ax.set_yticks(ticks)

    if y_axis.get("number_format") == "comma":
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _pos: f"{int(x):,}"))


def _line_style(name: str | None) -> str:
    """スタイル名を Matplotlib 用の線種へ変換する。"""

    return _LINESTYLE_MAP.get(str(name or "solid"), "-")


def _coerce_text(value: object) -> str:
    """文字列をstripして返す。"""

    return str(value or "").strip()


def _legend_label_visible(label: object) -> bool:
    """凡例に表示すべきラベルかを判定する。"""

    text = _coerce_text(label)
    return bool(text) and text != "_nolegend_"


def _format_24h_tick(x: float, _pos: int | None = None) -> str:
    """24時表記用のX軸ラベルを返す。"""

    dt = mdates.num2date(x)
    if dt.tzinfo is not None:
        dt = dt.replace(tzinfo=None)
    if dt.hour == 0 and dt.minute == 0 and dt.second == 0 and dt.microsecond == 0:
        dt = dt - timedelta(days=1)
        return dt.strftime("%m/%d 24")
    return dt.strftime("%m/%d %H")


def _is_24h_time_display_mode(value: object) -> bool:
    text = _coerce_text(value).lower()
    return text in {"24h", "24時", "24時表記", "1時~24時", "1時〜24時"}


def _time_column_for_plot(df: pd.DataFrame) -> str:
    """描画時に使う時刻列を返す。"""

    if "period_end_at" in df.columns:
        period_end = pd.to_datetime(df["period_end_at"], errors="coerce")
        if not period_end.isna().all():
            return "period_end_at"
    return "observed_at"
