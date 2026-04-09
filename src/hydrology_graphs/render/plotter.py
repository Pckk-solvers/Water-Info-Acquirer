"""水文グラフの Matplotlib 描画処理。

このモジュールは、グラフ種別ごとの見た目と基準線の重畳を担当する。
"""

from __future__ import annotations

from datetime import timedelta
from io import BytesIO
import math
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
_FIXED_BACKGROUND_COLOR = "#FFFFFF"

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
    if graph_type == GRAPH_HYETOGRAPH:
        return _render_hyetograph_png(
            station_name=station_name,
            df=df,
            graph_style=graph_style,
            thresholds=thresholds,
            time_display_mode=time_display_mode,
            figure_width=figure_width,
            figure_height=figure_height,
            dpi=dpi,
        )

    trimmed_df = _trim_event_dataframe(df, graph_style, graph_type=graph_type)

    fig, ax = plt.subplots(
        figsize=(figure_width, figure_height),
        dpi=dpi,
    )
    fig.patch.set_facecolor(_FIXED_BACKGROUND_COLOR)
    ax.set_facecolor(_FIXED_BACKGROUND_COLOR)
    _apply_common_axes_style(ax, graph_style, graph_type=graph_type)

    # グラフ種別ごとに描画方法を切り替える。イベント系は折れ線/棒、年最大系は年次棒。
    if graph_type in (GRAPH_HYDRO_DISCHARGE, GRAPH_HYDRO_WATER_LEVEL):
        _plot_hydro(ax, trimmed_df, graph_style)
    elif graph_type in (GRAPH_ANNUAL_RAINFALL, GRAPH_ANNUAL_DISCHARGE, GRAPH_ANNUAL_WATER_LEVEL):
        _plot_annual(ax, trimmed_df, graph_style)
    else:
        raise ValueError(f"Unsupported graph_type: {graph_type}")

    # 基準線は最後に重ねることで、主系列の見た目を邪魔しにくくする。
    _plot_thresholds(ax, thresholds, graph_style)
    _plot_date_boundaries(ax, trimmed_df, graph_style, graph_type=graph_type)
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


def _render_hyetograph_png(
    *,
    station_name: str,
    df: pd.DataFrame,
    graph_style: dict[str, Any],
    thresholds: list[ThresholdRecord],
    time_display_mode: str,
    figure_width: float,
    figure_height: float,
    dpi: int,
) -> bytes:
    """ハイエトグラフ専用の単一プロット+右軸描画。"""

    fig, ax_rain = plt.subplots(
        figsize=(figure_width, figure_height),
        dpi=dpi,
    )
    fig.patch.set_facecolor(_FIXED_BACKGROUND_COLOR)
    ax_rain.set_facecolor(_FIXED_BACKGROUND_COLOR)
    _apply_common_axes_style(ax_rain, graph_style, graph_type=GRAPH_HYETOGRAPH, allow_invert=True)

    trimmed_df = _trim_event_dataframe(df, graph_style, graph_type=GRAPH_HYETOGRAPH)
    full_time, bar_values, missing_mask = _prepare_hyetograph_data(trimmed_df)
    bar_cfg = graph_style.get("bar", {})
    bar_enabled = bool(bar_cfg.get("enabled", True))
    width_hours = float(graph_style.get("x_axis", {}).get("tick_interval_hours", 1))
    width = float(bar_cfg.get("width", 0.8)) / max(width_hours, 1.0)
    if bar_enabled:
        ax_rain.bar(
            full_time,
            bar_values,
            width=width,
            color=graph_style.get("bar_color", "#60A5FA"),
            edgecolor=(0.0, 0.0, 0.0, float(bar_cfg.get("edge_alpha", 1.0))),
            linewidth=float(bar_cfg.get("edge_width", 0.0)),
            label="時間雨量",
            zorder=float(graph_style.get("series", {}).get("zorder", 2)),
        )
    _plot_missing_bands(ax_rain, full_time, missing_mask, graph_style)

    _plot_thresholds(ax_rain, thresholds, graph_style)
    _plot_date_boundaries(ax_rain, trimmed_df, graph_style, graph_type=GRAPH_HYETOGRAPH)
    ax_cum = ax_rain.twinx()
    ax_cum.set_facecolor((1, 1, 1, 0))
    ax_cum.grid(False)

    cumulative_cfg = graph_style.get("cumulative_line", {})
    if bool(cumulative_cfg.get("enabled", False)):
        cumulative = bar_values.cumsum().where(~missing_mask)
        ax_cum.plot(
            full_time,
            cumulative,
            color=str(cumulative_cfg.get("color", graph_style.get("secondary_series_color", "#1E3A8A"))),
            linewidth=float(cumulative_cfg.get("width", 1.6)),
            linestyle=_line_style(str(cumulative_cfg.get("style", "solid"))),
            label="累積雨量",
            zorder=float(graph_style.get("series", {}).get("zorder", 2)) + 0.2,
        )
        cumulative_numeric = pd.Series(pd.to_numeric(cumulative, errors="coerce"), dtype="float64")
        non_null = cumulative_numeric.dropna()
        peak = float(non_null.max()) if not non_null.empty else 0.0
    else:
        peak = 0.0

    _apply_axis_details(
        ax_rain,
        graph_style,
        time_display_mode=time_display_mode,
        fixed_y_min=0.0,
        y_axis_override=graph_style.get("y_axis", {}),
        apply_x_axis=True,
    )
    y2_axis = graph_style.get("y2_axis", {})
    y2_max = y2_axis.get("max")
    if y2_max is None:
        tick_step = y2_axis.get("tick_step")
        if tick_step is not None:
            step = max(float(tick_step), 1e-6)
            upper = (math.ceil(peak / step) * step) if peak > 0 else step * 5
        else:
            upper = _nice_upper_bound(peak)
    else:
        upper = float(y2_max)
        if peak > upper:
            upper = _nice_upper_bound(peak)
    ax_cum.set_ylim(bottom=0.0, top=max(upper, 1.0))
    _sync_secondary_ticks_from_primary(ax_rain, ax_cum)
    if bool(graph_style.get("y_axis", {}).get("enabled", True)):
        _apply_y_axis_number_format(ax_cum, graph_style.get("y_axis", {}).get("number_format", "plain"))

    _apply_title_axis_labels(ax_rain, graph_style, station_name)
    y2_label = _coerce_text(graph_style.get("y2_axis", {}).get("label", "累積雨量"))
    ax_cum.set_ylabel(y2_label)
    if graph_style.get("invert_y_axis"):
        # 軸範囲調整後に反転を再適用し、棒・累積線の両方へ確実に反映する。
        ax_rain.invert_yaxis()
        ax_cum.invert_yaxis()
        ax_rain.xaxis.tick_top()

    legend_cfg = graph_style.get("legend", {})
    if legend_cfg.get("enabled", True):
        loc = str(legend_cfg.get("position", "upper right"))
        anchor = legend_cfg.get("fixed_anchor")
        handles_r, labels_r = ax_rain.get_legend_handles_labels()
        handles_c, labels_c = ax_cum.get_legend_handles_labels()
        handles = handles_r + handles_c
        labels = labels_r + labels_c
        filtered = [(h, label) for h, label in zip(handles, labels, strict=False) if _legend_label_visible(label)]
        handles = [h for h, _ in filtered]
        labels = [label for _, label in filtered]
        if isinstance(anchor, dict) and "x" in anchor and "y" in anchor:
            if handles:
                ax_rain.legend(handles, labels, loc=loc, bbox_to_anchor=(float(anchor["x"]), float(anchor["y"])))
        elif handles:
            ax_rain.legend(handles, labels, loc=loc)

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


def _nice_upper_bound(value: float) -> float:
    """値を見やすい上限へ丸める。"""

    if value <= 0:
        return 10.0
    scale = 1.0
    while value / scale > 10:
        scale *= 10
    for factor in (1.0, 2.0, 5.0, 10.0):
        candidate = factor * scale
        if value <= candidate:
            return candidate
    return 10.0 * scale


def _sync_secondary_ticks_from_primary(primary_ax, secondary_ax) -> None:
    """右軸目盛を左軸目盛に連動させる。"""

    left_min, left_max = primary_ax.get_ylim()
    right_min, right_max = secondary_ax.get_ylim()
    if left_max <= left_min or right_max <= right_min:
        return
    left_ticks_all = primary_ax.get_yticks()
    left_ticks = [tick for tick in left_ticks_all if left_min <= tick <= left_max]
    if not left_ticks:
        return
    mapped_ticks = [
        right_min + ((tick - left_min) / (left_max - left_min)) * (right_max - right_min)
        for tick in left_ticks
    ]
    secondary_ax.set_yticks(mapped_ticks)


def _apply_common_axes_style(
    ax,
    graph_style: dict[str, Any],
    *,
    graph_type: str,
    allow_invert: bool = True,
) -> None:
    """全グラフ共通の軸スタイルを当てる。"""

    font = graph_style.get("font", {})
    tick_size = float(font.get("tick_size", graph_style.get("font_size", 11)))
    ax.tick_params(labelsize=tick_size)
    grid = graph_style.get("grid", {})
    grid_enabled = bool(grid.get("enabled", True))
    x_grid_enabled = grid_enabled and bool(grid.get("x_enabled", grid.get("enabled", True)))
    y_grid_enabled = grid_enabled and bool(grid.get("y_enabled", grid.get("enabled", True)))
    if y_grid_enabled:
        ax.grid(
            True,
            axis="y",
            linestyle=str(grid.get("style", "--")),
            color=str(grid.get("color", "#CBD5E1")),
            alpha=float(grid.get("alpha", 0.7)),
        )
    else:
        ax.grid(False, axis="y")
    if x_grid_enabled:
        ax.grid(
            True,
            axis="x",
            linestyle=str(grid.get("style", "--")),
            color=str(grid.get("color", "#CBD5E1")),
            alpha=float(grid.get("alpha", 0.7)),
        )
    else:
        ax.grid(False, axis="x")
    if allow_invert and graph_style.get("invert_y_axis"):
        ax.invert_yaxis()
        ax.xaxis.tick_top()


def _prepare_hyetograph_data(df: pd.DataFrame) -> tuple[pd.DatetimeIndex, pd.Series, pd.Series]:
    """ハイエト描画用の時系列（全時刻・値・欠測マスク）を作る。"""

    if df.empty:
        return pd.DatetimeIndex([]), pd.Series(dtype="float64"), pd.Series(dtype="bool")
    time_col = _time_column_for_plot(df)
    data = df.sort_values(time_col).copy()
    data[time_col] = pd.to_datetime(data[time_col], errors="coerce")
    data = data.dropna(subset=[time_col]).reset_index(drop=True)
    if data.empty:
        return pd.DatetimeIndex([]), pd.Series(dtype="float64"), pd.Series(dtype="bool")
    full_time = pd.date_range(start=data[time_col].min(), end=data[time_col].max(), freq="1h")
    value_series = cast(pd.Series, pd.to_numeric(data["value"], errors="coerce"))
    indexed_values = pd.Series(value_series.values, index=data[time_col]).groupby(level=0).last()
    values = indexed_values.reindex(full_time)
    quality_missing = pd.Series(False, index=full_time)
    if "quality" in data.columns:
        q_series = data.set_index(time_col)["quality"]
        q_missing = q_series.eq("missing")
        quality_missing = q_missing.groupby(level=0).last().reindex(full_time).fillna(False).astype(bool)
    missing_mask = values.isna() | quality_missing
    bar_values = values.fillna(0.0)
    return full_time, bar_values, missing_mask


def _trim_event_dataframe(df: pd.DataFrame, graph_style: dict[str, Any], *, graph_type: str) -> pd.DataFrame:
    """イベント系グラフの描画前データを先頭/末尾トリムする。"""

    if graph_type not in {GRAPH_HYETOGRAPH, GRAPH_HYDRO_DISCHARGE, GRAPH_HYDRO_WATER_LEVEL}:
        return df
    if df.empty:
        return df

    x_axis = graph_style.get("x_axis", {})
    if not bool(x_axis.get("data_trim_enabled", True)):
        return df
    try:
        trim_start = max(float(x_axis.get("data_trim_start_hours", 0.0) or 0.0), 0.0)
    except Exception:  # noqa: BLE001
        trim_start = 0.0
    try:
        trim_end = max(float(x_axis.get("data_trim_end_hours", 0.0) or 0.0), 0.0)
    except Exception:  # noqa: BLE001
        trim_end = 0.0
    if trim_start <= 0 and trim_end <= 0:
        return df

    time_col = _time_column_for_plot(df)
    if time_col not in df.columns:
        return df

    data = df.copy()
    ts = pd.to_datetime(data[time_col], errors="coerce")
    valid = ts.dropna()
    if valid.empty:
        return data.iloc[0:0].copy()

    start_bound = valid.min() + pd.Timedelta(hours=trim_start)
    end_bound = valid.max() - pd.Timedelta(hours=trim_end)
    if end_bound < start_bound:
        return data.iloc[0:0].copy()

    mask = ts.between(start_bound, end_bound, inclusive="both")
    return data.loc[mask].copy()


def _plot_hydro(ax, df: pd.DataFrame, graph_style: dict[str, Any]) -> None:
    """流量・水位の折れ線グラフを描く。"""

    series_cfg = graph_style.get("series", {})
    if not bool(series_cfg.get("enabled", True)):
        return
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
        zorder=float(series_cfg.get("zorder", 2)),
    )


def _plot_annual(ax, df: pd.DataFrame, graph_style: dict[str, Any]) -> None:
    """年最大系の棒グラフを描く。"""

    bar_cfg = graph_style.get("bar", {})
    if not bool(bar_cfg.get("enabled", True)):
        return
    annual = annual_max_by_year(df) if "year" not in df.columns else df.copy()
    xs = [str(int(year)) for year in annual["year"].tolist()]
    ys = annual["value"].tolist()
    ax.bar(
        xs,
        ys,
        color=graph_style.get("bar_color", graph_style.get("series_color", "#1D4ED8")),
        width=float(bar_cfg.get("width", 0.8)),
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


def _apply_axis_details(
    ax,
    graph_style: dict[str, Any],
    *,
    time_display_mode: str = "datetime",
    fixed_y_min: float | None = None,
    y_axis_override: dict[str, Any] | None = None,
    apply_x_axis: bool = True,
) -> None:
    """日時目盛りや数値フォーマットなどの細部を調整する。"""

    x_axis = graph_style.get("x_axis", {})
    y_axis = y_axis_override if isinstance(y_axis_override, dict) else graph_style.get("y_axis", {})
    y_axis_enabled = bool(y_axis.get("enabled", True))
    if apply_x_axis:
        if x_axis.get("tick_interval_hours"):
            ax.xaxis.set_major_locator(mdates.HourLocator(interval=max(1, int(float(x_axis["tick_interval_hours"]))))
            )
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

    if y_axis_enabled:
        y_min = fixed_y_min if fixed_y_min is not None else y_axis.get("min")
        y_max = y_axis.get("max")
        if y_min is not None or y_max is not None:
            ax.set_ylim(bottom=y_min, top=y_max)

        tick_step = y_axis.get("tick_step")
        if tick_step is not None:
            tick_step = float(tick_step)
            low, high = ax.get_ylim()
            if tick_step > 0 and high > low:
                eps = max(abs(low), abs(high), 1.0) * 1e-9
                ticks: list[float] = []
                value = low
                while value <= high + eps:
                    ticks.append(value)
                    value += tick_step
                if ticks and abs(ticks[-1] - high) <= eps * 10:
                    ticks[-1] = high
                ax.set_yticks(ticks)

        _apply_y_axis_number_format(ax, y_axis.get("number_format", "plain"))


def _apply_y_axis_number_format(ax, number_format: object) -> None:
    """Y軸値形式を適用する。"""

    fmt = _coerce_text(number_format).lower() or "plain"
    if fmt == "comma":
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _pos: f"{int(round(x)):,}"))
        return
    if fmt == "percent":
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _pos: f"{x:g}%"))
        return
    scalar = mticker.ScalarFormatter(useOffset=False)
    scalar.set_scientific(False)
    ax.yaxis.set_major_formatter(scalar)


def _plot_missing_bands(ax, x_values: pd.DatetimeIndex, missing_mask: pd.Series, graph_style: dict[str, Any]) -> None:
    """欠測区間をグレー帯で重ねる。"""

    cfg = graph_style.get("missing_band", {})
    if not bool(cfg.get("enabled", False)):
        return
    if x_values.empty:
        return
    width_hours = float(graph_style.get("x_axis", {}).get("tick_interval_hours", 1))
    half = pd.Timedelta(hours=max(width_hours, 1.0) / 2.0)
    band_color = str(cfg.get("color", "#9CA3AF"))
    band_alpha = float(cfg.get("alpha", 0.28))
    mask = missing_mask.fillna(False).astype(bool)
    start = None
    for i, is_missing in enumerate(mask.tolist()):
        if is_missing and start is None:
            start = x_values[i]
        if (not is_missing) and start is not None:
            end = x_values[i - 1]
            ax.axvspan(start - half, end + half, color=band_color, alpha=band_alpha, zorder=1.4)
            start = None
    if start is not None:
        end = x_values[-1]
        ax.axvspan(start - half, end + half, color=band_color, alpha=band_alpha, zorder=1.4)


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
