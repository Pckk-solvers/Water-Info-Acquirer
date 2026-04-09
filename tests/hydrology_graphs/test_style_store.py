from __future__ import annotations

import json

from hydrology_graphs.io.style_store import default_style, load_style, save_style


def test_default_style_schema_2_0_has_9_graph_keys():
    style = default_style()
    assert style["schema_version"] == "2.0"
    assert len(style["graph_styles"]) == 9
    assert "hyetograph:3day" in style["graph_styles"]
    assert "hyetograph:5day" in style["graph_styles"]
    assert "annual_max_rainfall" in style["graph_styles"]
    assert style["display"]["time_display_mode"] == "datetime"


def test_default_style_x_axis_defaults_are_zero():
    style = default_style()
    for graph_style in style["graph_styles"].values():
        x_axis = graph_style.get("x_axis", {})
        grid = graph_style.get("grid", {})
        bar = graph_style.get("bar", {})
        y_axis = graph_style.get("y_axis", {})
        series = graph_style.get("series", {})
        assert x_axis.get("range_margin_rate", 0) == 0
        assert x_axis.get("tick_rotation", 0) == 0
        assert isinstance(x_axis.get("data_trim_enabled", True), bool)
        assert x_axis.get("data_trim_start_hours", 0) == 0
        assert x_axis.get("data_trim_end_hours", 0) == 0
        assert isinstance(grid.get("enabled", True), bool)
        assert isinstance(grid.get("x_enabled"), bool)
        assert isinstance(grid.get("y_enabled"), bool)
        assert isinstance(bar.get("enabled", True), bool)
        assert isinstance(y_axis.get("enabled", True), bool)
        assert isinstance(series.get("enabled", True), bool)
        assert "background_color" not in graph_style


def test_load_style_drops_removed_background_color_key():
    payload = default_style()
    payload["graph_styles"]["hyetograph:3day"]["background_color"] = "#000000"

    result = load_style(payload=payload)

    assert result.is_valid
    assert "background_color" not in result.style["graph_styles"]["hyetograph:3day"]


def test_load_style_rejects_invalid_schema():
    payload = default_style()
    payload["schema_version"] = "1.0"

    result = load_style(payload=payload)

    assert not result.is_valid
    assert any(message.startswith("error:unsupported_schema_version") for message in result.warnings)


def test_load_style_rejects_common_and_variants_in_schema_2_0():
    payload = default_style()
    payload["common"] = {"dpi": 100}
    payload["variants"] = {"hyetograph:3day": {"figure_width": 10}}

    result = load_style(payload=payload)

    assert not result.is_valid
    assert result.warnings
    assert result.warnings[0] in {"error:common_removed_in_schema_2_0", "error:variants_removed_in_schema_2_0"}


def test_load_style_allows_empty_title_and_axis_labels():
    payload = default_style()
    payload["graph_styles"]["hyetograph:3day"]["title"]["template"] = "   "
    payload["graph_styles"]["hyetograph:3day"]["axis"]["x_label"] = ""
    payload["graph_styles"]["hyetograph:3day"]["axis"]["y_label"] = " "

    result = load_style(payload=payload)

    assert result.is_valid


def test_load_style_backfills_display_time_display_mode():
    payload = default_style()
    payload.pop("display", None)

    result = load_style(payload=payload)

    assert result.is_valid
    assert result.style["display"]["time_display_mode"] == "datetime"


def test_load_style_normalizes_invalid_display_time_display_mode():
    payload = default_style()
    payload["display"] = {"time_display_mode": "invalid"}

    result = load_style(payload=payload)

    assert result.is_valid
    assert result.style["display"]["time_display_mode"] == "datetime"


def test_save_style_writes_schema_2_0(tmp_path):
    payload = default_style()
    out = tmp_path / "style.json"
    save_style(out, payload)
    saved = json.loads(out.read_text(encoding="utf-8"))
    assert saved["schema_version"] == "2.0"
    assert "common" not in saved
    assert "variants" not in saved


def test_load_style_rejects_negative_x_axis_range_margin_rate():
    payload = default_style()
    payload["graph_styles"]["hyetograph:3day"].setdefault("x_axis", {})["range_margin_rate"] = -0.01

    result = load_style(payload=payload)

    assert not result.is_valid
    assert "error:hyetograph:3day_x_axis_range_margin_rate_must_be_non_negative" in result.warnings


def test_load_style_rejects_invalid_series_style_by_schema():
    payload = default_style()
    payload["graph_styles"]["hyetograph:3day"]["series_style"] = "invalid-style"

    result = load_style(payload=payload)

    assert not result.is_valid
    assert "error:hyetograph:3day_series_style_invalid" in result.warnings


def test_load_style_backfills_null_optional_x_axis_margin_rate():
    payload = default_style()
    payload["graph_styles"]["annual_max_rainfall"].setdefault("x_axis", {})["range_margin_rate"] = None

    result = load_style(payload=payload)

    assert result.is_valid
    assert result.style["graph_styles"]["annual_max_rainfall"]["x_axis"]["range_margin_rate"] == 0


def test_load_style_backfills_date_boundary_line_defaults():
    payload = default_style()
    payload["graph_styles"]["hyetograph:3day"]["x_axis"].pop("date_boundary_line_enabled", None)
    payload["graph_styles"]["hyetograph:3day"]["x_axis"].pop("date_boundary_line_offset_hours", None)

    result = load_style(payload=payload)

    assert result.is_valid
    x_axis = result.style["graph_styles"]["hyetograph:3day"]["x_axis"]
    assert x_axis["date_boundary_line_enabled"] is False
    assert x_axis["date_boundary_line_offset_hours"] == 0.0


def test_load_style_backfills_data_trim_defaults():
    payload = default_style()
    payload["graph_styles"]["hydrograph_discharge:3day"]["x_axis"].pop("data_trim_start_hours", None)
    payload["graph_styles"]["hydrograph_discharge:3day"]["x_axis"].pop("data_trim_end_hours", None)

    result = load_style(payload=payload)

    assert result.is_valid
    x_axis = result.style["graph_styles"]["hydrograph_discharge:3day"]["x_axis"]
    assert x_axis["data_trim_enabled"] is True
    assert x_axis["data_trim_start_hours"] == 0.0
    assert x_axis["data_trim_end_hours"] == 0.0


def test_load_style_rejects_negative_x_axis_data_trim_start_hours():
    payload = default_style()
    payload["graph_styles"]["hyetograph:3day"]["x_axis"]["data_trim_start_hours"] = -0.5

    result = load_style(payload=payload)

    assert not result.is_valid
    assert "error:hyetograph:3day_x_axis_data_trim_start_hours_must_be_non_negative" in result.warnings


def test_default_hyetograph_style_contains_dual_axis_and_missing_band_defaults():
    style = default_style()["graph_styles"]["hyetograph:3day"]
    assert style["grid"]["x_enabled"] is False
    assert style["grid"]["y_enabled"] is True
    assert style["bar"]["edge_width"] > 0
    assert style["bar"]["edge_alpha"] > 0
    assert style["cumulative_line"]["enabled"] is True
    assert style["missing_band"]["enabled"] is True
    assert style["y2_axis"]["max"] > 0


def test_load_style_accepts_hyetograph_added_keys():
    payload = default_style()
    hyeto = payload["graph_styles"]["hyetograph:3day"]
    hyeto["grid"]["x_enabled"] = True
    hyeto["bar"]["edge_width"] = 1.2
    hyeto["bar"]["edge_alpha"] = 0.5
    hyeto["cumulative_line"]["style"] = "dashdot"
    hyeto["missing_band"]["alpha"] = 0.3
    hyeto["y2_axis"]["tick_step"] = 20

    result = load_style(payload=payload)

    assert result.is_valid
