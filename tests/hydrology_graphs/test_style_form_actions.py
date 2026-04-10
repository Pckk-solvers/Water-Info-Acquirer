from __future__ import annotations

from hydrology_graphs.io.style_store import default_style
from hydrology_graphs.ui.style_form_actions import apply_style_form_values, coerce_control_value


class _DummyVar:
    def __init__(self, value):
        self._value = value

    def get(self):
        return self._value

    def set(self, value):
        self._value = value


class _DummyApp:
    def __init__(self):
        self._style_payload = default_style()
        self._style_graph_controls = []
        self.time_display_mode = _DummyVar("datetime")
        self.preview_message = _DummyVar("")

    def _current_style_graph_key(self) -> str:
        return "hyetograph:3day"


def test_coerce_control_value_accepts_half_step_trim():
    control = {
        "path": "x_axis.data_trim_start_hours",
        "label": "表示データ範囲トリム",
        "kind": "float",
        "var": _DummyVar("1.5"),
    }

    value, error = coerce_control_value(control, current_value=None, empty_numeric=object())

    assert error is None
    assert value == 1.5


def test_coerce_control_value_rejects_non_half_step_trim():
    control = {
        "path": "x_axis.data_trim_end_hours",
        "label": "表示データ範囲トリム",
        "kind": "float",
        "var": _DummyVar("1.25"),
    }

    value, error = coerce_control_value(control, current_value=0.0, empty_numeric=object())

    assert value == 0.0
    assert error is not None
    assert "0.5刻み" in error


def test_apply_style_form_values_keeps_axis_grid_toggle_values():
    app = _DummyApp()
    app._style_graph_controls = [
        {"path": "grid.x_enabled", "label": "grid.x_enabled", "kind": "bool", "var": _DummyVar(True)},
        {"path": "grid.y_enabled", "label": "grid.y_enabled", "kind": "bool", "var": _DummyVar(False)},
    ]

    result = apply_style_form_values(app, empty_numeric=object(), valid_time_display_modes={"datetime", "24h"})

    assert result.ok is True
    assert {"grid.x_enabled", "grid.y_enabled"}.issubset(result.changed_paths)
    graph_style = app._style_payload["graph_styles"]["hyetograph:3day"]
    assert graph_style["grid"]["x_enabled"] is True
    assert graph_style["grid"]["y_enabled"] is False


def test_apply_style_form_values_returns_time_display_mode_change_path():
    app = _DummyApp()
    app.time_display_mode.set("24h")

    result = apply_style_form_values(app, empty_numeric=object(), valid_time_display_modes={"datetime", "24h"})

    assert result.ok is True
    assert "display.time_display_mode" in result.changed_paths
