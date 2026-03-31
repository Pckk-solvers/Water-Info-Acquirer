from __future__ import annotations

from io import BytesIO
from typing import Any
import warnings

import matplotlib

matplotlib.use("Agg")

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
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
import japanize_matplotlib  # noqa: F401  # Matplotlib の日本語フォント設定を有効化する
import pandas as pd

"""水文グラフの Matplotlib 描画処理。

このモジュールは、グラフ種別ごとの見た目と基準線の重畳を担当する。
"""

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

_LINESTYLE_MAP = {"solid": "-", "dashed": "--", "dotted": ":"}


def render_graph_png(
    *,
    graph_type: str,
    station_name: str,
    df: pd.DataFrame,
    style: dict[str, Any],
    thresholds: list[ThresholdRecord],
) -> bytes:
    """描画対象とスタイルから PNG バイト列を生成する。"""

    common = style["common"]
    graph_style = style["graph_styles"][graph_type]
    fig, ax = plt.subplots(
        figsize=(float(common["figure_width"]), float(common["figure_height"])),
        dpi=int(common.get("dpi", 120)),
    )
    fig.patch.set_facecolor(common.get("background_color", "#FFFFFF"))
    ax.set_facecolor(common.get("background_color", "#FFFFFF"))
    _apply_common_axes_style(ax, common, graph_style)

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
    _apply_title_axis_labels(ax, graph_style, station_name)
    _apply_axis_details(ax, graph_style)

    legend_cfg = common.get("legend", {})
    if legend_cfg.get("enabled", True):
        loc = str(legend_cfg.get("position", "upper right"))
        anchor = legend_cfg.get("fixed_anchor")
        # 位置を固定したい場合は bbox_to_anchor を使い、通常は標準位置を使う。
        if isinstance(anchor, dict) and "x" in anchor and "y" in anchor:
            ax.legend(loc=loc, bbox_to_anchor=(float(anchor["x"]), float(anchor["y"])))
        elif ax.get_legend_handles_labels()[0]:
            ax.legend(loc=loc)

    margin = common.get("margin", {})
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
        transparent=bool(common.get("export", {}).get("transparent_background", False)),
    )
    plt.close(fig)
    return buf.getvalue()


def _apply_common_axes_style(ax, common: dict[str, Any], graph_style: dict[str, Any]) -> None:
    """全グラフ共通の軸スタイルを当てる。"""

    font = common.get("font", {})
    tick_size = float(font.get("tick_size", common.get("font_size", 11)))
    ax.tick_params(labelsize=tick_size)
    grid = common.get("grid", {})
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

    data = df.sort_values("observed_at").copy()
    data["observed_at"] = pd.to_datetime(data["observed_at"], errors="coerce")
    data["value"] = pd.to_numeric(data["value"], errors="coerce").fillna(0.0)
    bar_cfg = graph_style.get("bar", {})
    width_hours = float(graph_style.get("x_axis", {}).get("tick_interval_hours", 1))
    width = float(bar_cfg.get("width", 0.8)) / max(width_hours, 1.0)
    ax.bar(
        data["observed_at"],
        data["value"],
        width=width,
        color=graph_style.get("bar_color", "#60A5FA"),
        label="時間雨量",
        zorder=float(graph_style.get("series", {}).get("zorder", 2)),
    )


def _plot_hydro(ax, df: pd.DataFrame, graph_style: dict[str, Any]) -> None:
    """流量・水位の折れ線グラフを描く。"""

    data = df.sort_values("observed_at").copy()
    data["observed_at"] = pd.to_datetime(data["observed_at"], errors="coerce")
    ax.plot(
        data["observed_at"],
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
        ax.axhline(
            y=line.value,
            color=color,
            linestyle=linestyle,
            linewidth=float(line.line_width or 1.2),
            zorder=zorder,
            label=line.label or line.line_name,
        )
        if label_enabled:
            # ラベルは左寄せで重ならない位置に置く。
            ymax = ax.get_ylim()[1]
            offset = (abs(ymax) if ymax != 0 else 1.0) * label_offset
            ax.text(
                0.01,
                line.value + offset,
                line.label or line.line_name,
                transform=ax.get_yaxis_transform(),
                color=color,
                fontsize=label_size,
            )


def _apply_title_axis_labels(ax, graph_style: dict[str, Any], station_name: str) -> None:
    """タイトルと軸ラベルを反映する。"""

    title_cfg = graph_style.get("title", {})
    title_tpl = str(title_cfg.get("template", "{station_name}"))
    ax.set_title(title_tpl.format(station_name=station_name or ""))
    axis = graph_style.get("axis", {})
    ax.set_xlabel(str(axis.get("x_label", "")))
    ax.set_ylabel(str(axis.get("y_label", "")))


def _apply_axis_details(ax, graph_style: dict[str, Any]) -> None:
    """日時目盛りや数値フォーマットなどの細部を調整する。"""

    x_axis = graph_style.get("x_axis", {})
    y_axis = graph_style.get("y_axis", {})
    if x_axis.get("date_format"):
        ax.xaxis.set_major_formatter(mdates.DateFormatter(str(x_axis["date_format"])))
    if x_axis.get("tick_interval_hours"):
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=max(1, int(float(x_axis["tick_interval_hours"])))))
    rotation = float(x_axis.get("tick_rotation", 0))
    align = str(x_axis.get("label_align", "center"))
    for tick in ax.get_xticklabels():
        tick.set_rotation(rotation)
        tick.set_horizontalalignment(align)

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
