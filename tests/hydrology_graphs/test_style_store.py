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
