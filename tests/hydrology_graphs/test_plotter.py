from __future__ import annotations

from datetime import datetime, timedelta

import matplotlib.axes
import pandas as pd

from hydrology_graphs.io.style_store import default_style
from hydrology_graphs.render.plotter import render_graph_png
from hydrology_graphs.ui.app import BASE_GRAPH_STYLE_FIELDS


def test_base_graph_style_fields_includes_x_axis_range_margin_rate_after_tick_rotation():
    paths = [field.get("path") for field in BASE_GRAPH_STYLE_FIELDS]
    rotation_index = paths.index("x_axis.tick_rotation")
    assert paths[rotation_index + 1] == "x_axis.range_margin_rate"
    assert paths[rotation_index + 2] == "x_axis.date_boundary_line_enabled"
    assert paths[rotation_index + 3] == "x_axis.date_boundary_line_offset_hours"


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
