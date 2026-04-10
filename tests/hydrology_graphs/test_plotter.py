from __future__ import annotations

from datetime import datetime, timedelta

import matplotlib.axes
import matplotlib.axis
import matplotlib.dates
import matplotlib.figure
import matplotlib.ticker as mticker
import pandas as pd

from hydrology_graphs.io.style_store import default_style
from hydrology_graphs.render.plotter import render_graph_png
from hydrology_graphs.ui.app import BASE_GRAPH_STYLE_FIELDS


def test_base_graph_style_fields_contains_axis_offset_and_tick_pad_fields():
    paths = [field.get("path") for field in BASE_GRAPH_STYLE_FIELDS]
    assert "background_color" not in paths
    assert "font_size" not in paths
    assert "grid.enabled" not in paths
    assert "axis.x_label_offset" in paths
    assert "axis.y_label_offset" in paths
    assert "y2_axis.label" in paths
    assert "y2_axis.label_rotation" in paths
    assert "x_axis.tick_label_pad" in paths
    assert "y_axis.tick_label_pad" in paths


def test_render_graph_png_applies_x_axis_range_margin_rate(monkeypatch):
    rows = []
    start = datetime(2025, 1, 1, 0, 0, 0)
    for i in range(24):
        rows.append(
            {
                "observed_at": start + timedelta(hours=i),
                "value": float(i),
            }
        )
    df = pd.DataFrame(rows)
    style = default_style()["graph_styles"]["hydrograph_discharge:3day"]
    style.setdefault("x_axis", {})["range_margin_rate"] = 0.12

    called: list[float] = []
    original_margins = matplotlib.axes.Axes.margins

    def _spy(self, *args, **kwargs):
        if "x" in kwargs:
            called.append(float(kwargs["x"]))
        return original_margins(self, *args, **kwargs)

    monkeypatch.setattr(matplotlib.axes.Axes, "margins", _spy)

    png = render_graph_png(
        graph_type="hydrograph_discharge",
        station_name="A",
        df=df,
        graph_style=style,
        thresholds=[],
        time_display_mode="datetime",
    )

    assert png[:8] == b"\x89PNG\r\n\x1a\n"
    assert called
    assert called[-1] == 0.12


def test_render_graph_png_uses_tick_hours_of_day_when_specified(monkeypatch):
    rows = []
    start = datetime(2025, 1, 1, 0, 0, 0)
    for i in range(24):
        rows.append(
            {
                "observed_at": start + timedelta(hours=i),
                "value": float(i),
            }
        )
    df = pd.DataFrame(rows)
    style = default_style()["graph_styles"]["hydrograph_discharge:3day"]
    style.setdefault("x_axis", {})["tick_hours_of_day"] = "1,2,3,4"

    captured: list[tuple[int, ...]] = []
    original_hour_locator = matplotlib.dates.HourLocator

    def _spy(*args, **kwargs):
        byhour = kwargs.get("byhour")
        if isinstance(byhour, (list, tuple)):
            captured.append(tuple(int(v) for v in byhour))
        return original_hour_locator(*args, **kwargs)

    monkeypatch.setattr(matplotlib.dates, "HourLocator", _spy)

    png = render_graph_png(
        graph_type="hydrograph_discharge",
        station_name="A",
        df=df,
        graph_style=style,
        thresholds=[],
        time_display_mode="datetime",
    )

    assert png[:8] == b"\x89PNG\r\n\x1a\n"
    assert captured
    assert captured[-1] == (0, 1, 2, 3)


def test_render_graph_png_hides_date_labels_when_show_date_labels_false(monkeypatch):
    rows = []
    start = datetime(2025, 1, 1, 0, 0, 0)
    for i in range(24):
        rows.append({"observed_at": start + timedelta(hours=i), "value": float(i)})
    df = pd.DataFrame(rows)
    style = default_style()["graph_styles"]["hydrograph_discharge:3day"]
    style.setdefault("x_axis", {})["show_date_labels"] = False

    formats: list[str] = []
    original_date_formatter = matplotlib.dates.DateFormatter

    def _spy(fmt, *args, **kwargs):
        formats.append(str(fmt))
        return original_date_formatter(fmt, *args, **kwargs)

    monkeypatch.setattr(matplotlib.dates, "DateFormatter", _spy)

    png = render_graph_png(
        graph_type="hydrograph_discharge",
        station_name="A",
        df=df,
        graph_style=style,
        thresholds=[],
        time_display_mode="datetime",
    )

    assert png[:8] == b"\x89PNG\r\n\x1a\n"
    assert formats
    assert formats[-1] == "%H:%M"


def test_render_annual_applies_year_tick_step(monkeypatch):
    rows = []
    for year in range(2018, 2024):
        rows.append({"year": year, "value": float(year - 2000)})
    df = pd.DataFrame(rows)
    style = default_style()["graph_styles"]["annual_max_rainfall"]
    style.setdefault("x_axis", {})["year_tick_step"] = 2

    captured_labels: list[list[str]] = []
    original_set_xticklabels = matplotlib.axes.Axes.set_xticklabels

    def _spy(self, labels, *args, **kwargs):
        captured_labels.append([str(label) for label in labels])
        return original_set_xticklabels(self, labels, *args, **kwargs)

    monkeypatch.setattr(matplotlib.axes.Axes, "set_xticklabels", _spy)

    png = render_graph_png(
        graph_type="annual_max_rainfall",
        station_name="A",
        df=df,
        graph_style=style,
        thresholds=[],
        time_display_mode="datetime",
    )

    assert png[:8] == b"\x89PNG\r\n\x1a\n"
    assert captured_labels
    labels = captured_labels[-1]
    assert labels[0] == "2018"
    assert labels[1] == ""
    assert labels[2] == "2020"


def test_render_graph_png_draws_date_boundaries_when_enabled(monkeypatch):
    rows = []
    start = datetime(2025, 1, 1, 0, 0, 0)
    for i in range(72):
        rows.append(
            {
                "observed_at": start + timedelta(hours=i),
                "value": float(i),
            }
        )
    df = pd.DataFrame(rows)
    style = default_style()["graph_styles"]["hydrograph_discharge:3day"]
    x_axis = style.setdefault("x_axis", {})
    x_axis["date_boundary_line_enabled"] = True
    x_axis["date_boundary_line_offset_hours"] = 0.0

    calls: list[object] = []
    original_axvline = matplotlib.axes.Axes.axvline

    def _spy(self, x, *args, **kwargs):
        calls.append(x)
        return original_axvline(self, x, *args, **kwargs)

    monkeypatch.setattr(matplotlib.axes.Axes, "axvline", _spy)

    png = render_graph_png(
        graph_type="hydrograph_discharge",
        station_name="A",
        df=df,
        graph_style=style,
        thresholds=[],
        time_display_mode="datetime",
    )

    assert png[:8] == b"\x89PNG\r\n\x1a\n"
    assert calls


def test_render_graph_png_draws_date_boundary_at_range_edges(monkeypatch):
    rows = []
    start = datetime(2025, 1, 1, 0, 0, 0)
    for i in range(24):
        rows.append(
            {
                "observed_at": start + timedelta(hours=i),
                "value": float(i),
            }
        )
    df = pd.DataFrame(rows)
    style = default_style()["graph_styles"]["hydrograph_discharge:3day"]
    x_axis = style.setdefault("x_axis", {})
    x_axis["date_boundary_line_enabled"] = True
    x_axis["date_boundary_line_offset_hours"] = 0.0

    calls: list[pd.Timestamp] = []
    original_axvline = matplotlib.axes.Axes.axvline

    def _spy(self, x, *args, **kwargs):
        calls.append(pd.to_datetime(x))
        return original_axvline(self, x, *args, **kwargs)

    monkeypatch.setattr(matplotlib.axes.Axes, "axvline", _spy)

    png = render_graph_png(
        graph_type="hydrograph_discharge",
        station_name="A",
        df=df,
        graph_style=style,
        thresholds=[],
        time_display_mode="datetime",
    )

    assert png[:8] == b"\x89PNG\r\n\x1a\n"
    assert calls
    assert min(calls) == pd.Timestamp("2025-01-01 00:00:00")


def test_render_graph_png_y_axis_ticks_do_not_exceed_max_when_tick_step_set(monkeypatch):
    rows = []
    start = datetime(2025, 1, 1, 0, 0, 0)
    for i in range(24):
        rows.append(
            {
                "observed_at": start + timedelta(hours=i),
                "value": float(i),
            }
        )
    df = pd.DataFrame(rows)
    style = default_style()["graph_styles"]["hydrograph_discharge:3day"]
    style.setdefault("y_axis", {})["max"] = 80
    style["y_axis"]["tick_step"] = 10

    captured: list[list[float]] = []
    original_set_yticks = matplotlib.axes.Axes.set_yticks

    def _spy(self, ticks, *args, **kwargs):
        captured.append([float(v) for v in ticks])
        return original_set_yticks(self, ticks, *args, **kwargs)

    monkeypatch.setattr(matplotlib.axes.Axes, "set_yticks", _spy)

    png = render_graph_png(
        graph_type="hydrograph_discharge",
        station_name="A",
        df=df,
        graph_style=style,
        thresholds=[],
        time_display_mode="datetime",
    )

    assert png[:8] == b"\x89PNG\r\n\x1a\n"
    assert captured
    assert max(captured[-1]) <= 80.0
    assert 90.0 not in captured[-1]


def test_render_hyetograph_draws_missing_band_when_missing_exists(monkeypatch):
    rows = []
    start = datetime(2025, 1, 1, 0, 0, 0)
    for i in range(6):
        rows.append(
            {
                "observed_at": start + timedelta(hours=i),
                "value": float(i + 1),
                "quality": "ok",
            }
        )
    rows[2]["quality"] = "missing"
    df = pd.DataFrame(rows)
    style = default_style()["graph_styles"]["hyetograph:3day"]
    style["missing_band"]["enabled"] = True

    calls: list[object] = []
    original_axvspan = matplotlib.axes.Axes.axvspan

    def _spy(self, *args, **kwargs):
        calls.append((args, kwargs))
        return original_axvspan(self, *args, **kwargs)

    monkeypatch.setattr(matplotlib.axes.Axes, "axvspan", _spy)

    png = render_graph_png(
        graph_type="hyetograph",
        station_name="A",
        df=df,
        graph_style=style,
        thresholds=[],
        time_display_mode="datetime",
    )

    assert png[:8] == b"\x89PNG\r\n\x1a\n"
    assert calls


def test_render_hyetograph_applies_comma_format_to_both_y_axes(monkeypatch):
    rows = []
    start = datetime(2025, 1, 1, 0, 0, 0)
    for i in range(12):
        rows.append(
            {
                "observed_at": start + timedelta(hours=i),
                "value": float(i + 1),
                "quality": "ok",
            }
        )
    df = pd.DataFrame(rows)
    style = default_style()["graph_styles"]["hyetograph:3day"]
    style.setdefault("y_axis", {})["number_format"] = "comma"

    y_formatters: list[mticker.FuncFormatter] = []
    original_set_major_formatter = matplotlib.axis.Axis.set_major_formatter

    def _spy(self, formatter):
        if getattr(self, "axis_name", "") == "y" and isinstance(formatter, mticker.FuncFormatter):
            y_formatters.append(formatter)
        return original_set_major_formatter(self, formatter)

    monkeypatch.setattr(matplotlib.axis.Axis, "set_major_formatter", _spy)

    png = render_graph_png(
        graph_type="hyetograph",
        station_name="A",
        df=df,
        graph_style=style,
        thresholds=[],
        time_display_mode="datetime",
    )

    assert png[:8] == b"\x89PNG\r\n\x1a\n"
    assert len(y_formatters) >= 2
    assert all(isinstance(fmt, mticker.FuncFormatter) for fmt in y_formatters[-2:])
    assert y_formatters[-1](1234.2, 0) == "1,234"


def test_render_hyetograph_applies_percent_format_to_both_y_axes(monkeypatch):
    rows = []
    start = datetime(2025, 1, 1, 0, 0, 0)
    for i in range(8):
        rows.append(
            {
                "observed_at": start + timedelta(hours=i),
                "value": float(i + 1),
                "quality": "ok",
            }
        )
    df = pd.DataFrame(rows)
    style = default_style()["graph_styles"]["hyetograph:3day"]
    style.setdefault("y_axis", {})["number_format"] = "percent"

    y_formatters: list[mticker.FuncFormatter] = []
    original_set_major_formatter = matplotlib.axis.Axis.set_major_formatter

    def _spy(self, formatter):
        if getattr(self, "axis_name", "") == "y" and isinstance(formatter, mticker.FuncFormatter):
            y_formatters.append(formatter)
        return original_set_major_formatter(self, formatter)

    monkeypatch.setattr(matplotlib.axis.Axis, "set_major_formatter", _spy)

    png = render_graph_png(
        graph_type="hyetograph",
        station_name="A",
        df=df,
        graph_style=style,
        thresholds=[],
        time_display_mode="datetime",
    )

    assert png[:8] == b"\x89PNG\r\n\x1a\n"
    assert len(y_formatters) >= 2
    assert all(isinstance(fmt, mticker.FuncFormatter) for fmt in y_formatters[-2:])
    assert y_formatters[-1](12.5, 0) == "12.5%"


def test_render_hyetograph_applies_custom_y2_label(monkeypatch):
    rows = []
    start = datetime(2025, 1, 1, 0, 0, 0)
    for i in range(8):
        rows.append(
            {
                "observed_at": start + timedelta(hours=i),
                "value": float(i + 1),
                "quality": "ok",
            }
        )
    df = pd.DataFrame(rows)
    style = default_style()["graph_styles"]["hyetograph:3day"]
    style.setdefault("y2_axis", {})["label"] = "累積雨量(mm)"

    labels: list[str] = []
    original_set_ylabel = matplotlib.axes.Axes.set_ylabel

    def _spy(self, ylabel, *args, **kwargs):
        labels.append(str(ylabel))
        return original_set_ylabel(self, ylabel, *args, **kwargs)

    monkeypatch.setattr(matplotlib.axes.Axes, "set_ylabel", _spy)

    png = render_graph_png(
        graph_type="hyetograph",
        station_name="A",
        df=df,
        graph_style=style,
        thresholds=[],
        time_display_mode="datetime",
    )

    assert png[:8] == b"\x89PNG\r\n\x1a\n"
    assert "累積雨量(mm)" in labels


def test_render_hyetograph_applies_y2_label_rotation_preset(monkeypatch):
    rows = []
    start = datetime(2025, 1, 1, 0, 0, 0)
    for i in range(8):
        rows.append(
            {
                "observed_at": start + timedelta(hours=i),
                "value": float(i + 1),
                "quality": "ok",
            }
        )
    df = pd.DataFrame(rows)
    style = default_style()["graph_styles"]["hyetograph:3day"]
    style.setdefault("y2_axis", {})["label_rotation"] = "180"

    rotation_values: list[float] = []
    original_set_ylabel = matplotlib.axes.Axes.set_ylabel

    def _spy(self, ylabel, *args, **kwargs):
        if "rotation" in kwargs:
            rotation_values.append(float(kwargs["rotation"]))
        return original_set_ylabel(self, ylabel, *args, **kwargs)

    monkeypatch.setattr(matplotlib.axes.Axes, "set_ylabel", _spy)

    png = render_graph_png(
        graph_type="hyetograph",
        station_name="A",
        df=df,
        graph_style=style,
        thresholds=[],
        time_display_mode="datetime",
    )

    assert png[:8] == b"\x89PNG\r\n\x1a\n"
    assert rotation_values
    assert rotation_values[-1] == 180.0


def test_render_hyetograph_expands_right_margin_for_long_y2_label(monkeypatch):
    rows = []
    start = datetime(2025, 1, 1, 0, 0, 0)
    for i in range(8):
        rows.append(
            {
                "observed_at": start + timedelta(hours=i),
                "value": float(i + 1),
                "quality": "ok",
            }
        )
    df = pd.DataFrame(rows)
    style = default_style()["graph_styles"]["hyetograph:3day"]
    style.setdefault("margin", {})["right"] = 0.01
    style.setdefault("y2_axis", {})["label"] = "累積雨量（右軸）長いラベル"
    style.setdefault("y_axis", {})["tick_label_pad"] = 16.0

    captured_right: list[float] = []
    original_adjust = matplotlib.figure.Figure.subplots_adjust

    def _spy(self, *args, **kwargs):
        if "right" in kwargs:
            captured_right.append(float(kwargs["right"]))
        return original_adjust(self, *args, **kwargs)

    monkeypatch.setattr(matplotlib.figure.Figure, "subplots_adjust", _spy)

    png = render_graph_png(
        graph_type="hyetograph",
        station_name="A",
        df=df,
        graph_style=style,
        thresholds=[],
        time_display_mode="datetime",
    )

    assert png[:8] == b"\x89PNG\r\n\x1a\n"
    assert captured_right
    assert captured_right[-1] < 0.9


def test_render_hydro_applies_x_axis_data_trim_hours(monkeypatch):
    rows = []
    start = datetime(2025, 1, 1, 0, 0, 0)
    for i in range(8):
        rows.append(
            {
                "observed_at": start + timedelta(hours=i),
                "value": float(i + 1),
            }
        )
    df = pd.DataFrame(rows)
    style = default_style()["graph_styles"]["hydrograph_discharge:3day"]
    x_axis = style.setdefault("x_axis", {})
    x_axis["data_trim_start_hours"] = 1.0
    x_axis["data_trim_end_hours"] = 2.0

    plotted_x: list[pd.Timestamp] = []
    original_plot = matplotlib.axes.Axes.plot

    def _spy(self, x, y, *args, **kwargs):
        plotted_x.extend(pd.to_datetime(x).tolist())
        return original_plot(self, x, y, *args, **kwargs)

    monkeypatch.setattr(matplotlib.axes.Axes, "plot", _spy)

    png = render_graph_png(
        graph_type="hydrograph_discharge",
        station_name="A",
        df=df,
        graph_style=style,
        thresholds=[],
        time_display_mode="datetime",
    )

    assert png[:8] == b"\x89PNG\r\n\x1a\n"
    assert plotted_x
    assert min(plotted_x) == start + timedelta(hours=1)
    assert max(plotted_x) == start + timedelta(hours=5)


def test_render_hydro_does_not_trim_when_data_trim_disabled(monkeypatch):
    rows = []
    start = datetime(2025, 1, 1, 0, 0, 0)
    for i in range(8):
        rows.append({"observed_at": start + timedelta(hours=i), "value": float(i + 1)})
    df = pd.DataFrame(rows)
    style = default_style()["graph_styles"]["hydrograph_discharge:3day"]
    x_axis = style.setdefault("x_axis", {})
    x_axis["data_trim_enabled"] = False
    x_axis["data_trim_start_hours"] = 1.0
    x_axis["data_trim_end_hours"] = 2.0

    plotted_x: list[pd.Timestamp] = []
    original_plot = matplotlib.axes.Axes.plot

    def _spy(self, x, y, *args, **kwargs):
        plotted_x.extend(pd.to_datetime(x).tolist())
        return original_plot(self, x, y, *args, **kwargs)

    monkeypatch.setattr(matplotlib.axes.Axes, "plot", _spy)

    png = render_graph_png(
        graph_type="hydrograph_discharge",
        station_name="A",
        df=df,
        graph_style=style,
        thresholds=[],
        time_display_mode="datetime",
    )

    assert png[:8] == b"\x89PNG\r\n\x1a\n"
    assert plotted_x
    assert min(plotted_x) == start
    assert max(plotted_x) == start + timedelta(hours=7)


def test_render_hyetograph_applies_half_hour_data_trim_to_bars(monkeypatch):
    rows = []
    start = datetime(2025, 1, 1, 0, 0, 0)
    for i in range(6):
        rows.append(
            {
                "observed_at": start + timedelta(hours=i),
                "value": float(i + 1),
                "quality": "ok",
            }
        )
    df = pd.DataFrame(rows)
    style = default_style()["graph_styles"]["hyetograph:3day"]
    x_axis = style.setdefault("x_axis", {})
    x_axis["data_trim_start_hours"] = 0.5
    x_axis["data_trim_end_hours"] = 0.5

    bar_x: list[pd.Timestamp] = []
    original_bar = matplotlib.axes.Axes.bar

    def _spy(self, x, *args, **kwargs):
        bar_x.extend(pd.to_datetime(x).tolist())
        return original_bar(self, x, *args, **kwargs)

    monkeypatch.setattr(matplotlib.axes.Axes, "bar", _spy)

    png = render_graph_png(
        graph_type="hyetograph",
        station_name="A",
        df=df,
        graph_style=style,
        thresholds=[],
        time_display_mode="datetime",
    )

    assert png[:8] == b"\x89PNG\r\n\x1a\n"
    assert bar_x
    assert min(bar_x) == start + timedelta(hours=1)
    assert max(bar_x) == start + timedelta(hours=4)


def test_render_hyetograph_bar_width_is_independent_from_tick_interval_hours(monkeypatch):
    rows = []
    start = datetime(2025, 1, 1, 0, 0, 0)
    for i in range(6):
        rows.append(
            {
                "observed_at": start + timedelta(hours=i),
                "value": float(i + 1),
                "quality": "ok",
            }
        )
    df = pd.DataFrame(rows)
    style = default_style()["graph_styles"]["hyetograph:3day"]
    style.setdefault("x_axis", {})["tick_interval_hours"] = 6
    style.setdefault("bar", {})["width"] = 0.25

    widths: list[float] = []
    original_bar = matplotlib.axes.Axes.bar

    def _spy(self, x, height, *args, **kwargs):
        widths.append(float(kwargs.get("width", 0.0)))
        return original_bar(self, x, height, *args, **kwargs)

    monkeypatch.setattr(matplotlib.axes.Axes, "bar", _spy)

    png = render_graph_png(
        graph_type="hyetograph",
        station_name="A",
        df=df,
        graph_style=style,
        thresholds=[],
        time_display_mode="datetime",
    )

    assert png[:8] == b"\x89PNG\r\n\x1a\n"
    assert widths
    assert widths[-1] == 0.25


def test_render_hydro_uses_axis_specific_grid_flags(monkeypatch):
    rows = []
    start = datetime(2025, 1, 1, 0, 0, 0)
    for i in range(8):
        rows.append({"observed_at": start + timedelta(hours=i), "value": float(i + 1)})
    df = pd.DataFrame(rows)
    style = default_style()["graph_styles"]["hydrograph_discharge:3day"]
    style.setdefault("grid", {})["x_enabled"] = False
    style["grid"]["y_enabled"] = True

    calls: list[tuple[object, object]] = []
    original_grid = matplotlib.axes.Axes.grid

    def _spy(self, visible=None, which="major", axis="both", **kwargs):
        calls.append((visible, axis))
        return original_grid(self, visible=visible, which=which, axis=axis, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(matplotlib.axes.Axes, "grid", _spy)

    png = render_graph_png(
        graph_type="hydrograph_discharge",
        station_name="A",
        df=df,
        graph_style=style,
        thresholds=[],
        time_display_mode="datetime",
    )

    assert png[:8] == b"\x89PNG\r\n\x1a\n"
    assert (False, "x") in calls
    assert (True, "y") in calls


def test_render_hydro_axis_grid_flags_take_precedence_over_grid_enabled(monkeypatch):
    rows = []
    start = datetime(2025, 1, 1, 0, 0, 0)
    for i in range(8):
        rows.append({"observed_at": start + timedelta(hours=i), "value": float(i + 1)})
    df = pd.DataFrame(rows)
    style = default_style()["graph_styles"]["hydrograph_discharge:3day"]
    style.setdefault("grid", {})["enabled"] = False
    style["grid"]["x_enabled"] = True
    style["grid"]["y_enabled"] = True

    calls: list[tuple[object, object]] = []
    original_grid = matplotlib.axes.Axes.grid

    def _spy(self, visible=None, which="major", axis="both", **kwargs):
        calls.append((visible, axis))
        return original_grid(self, visible=visible, which=which, axis=axis, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(matplotlib.axes.Axes, "grid", _spy)

    png = render_graph_png(
        graph_type="hydrograph_discharge",
        station_name="A",
        df=df,
        graph_style=style,
        thresholds=[],
        time_display_mode="datetime",
    )

    assert png[:8] == b"\x89PNG\r\n\x1a\n"
    assert (True, "x") in calls
    assert (True, "y") in calls
