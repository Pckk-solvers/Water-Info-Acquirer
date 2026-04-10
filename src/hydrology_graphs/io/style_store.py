from __future__ import annotations

import json
import re
from copy import deepcopy
from dataclasses import dataclass
from functools import lru_cache
from importlib import resources
from pathlib import Path
from typing import Any, cast

from jsonschema import Draft202012Validator

from ..domain.constants import (
    EVENT_GRAPH_TYPES,
    GRAPH_HYDRO_DISCHARGE,
    GRAPH_HYDRO_WATER_LEVEL,
    GRAPH_TYPES,
)

"""描画スタイル JSON の読込・保存・正規化。"""


EVENT_STYLE_KEYS: tuple[str, ...] = tuple(f"{graph_type}:{days}day" for graph_type in EVENT_GRAPH_TYPES for days in (3, 5))
ANNUAL_STYLE_KEYS: tuple[str, ...] = tuple(
    graph_type for graph_type in GRAPH_TYPES if graph_type not in EVENT_GRAPH_TYPES
)
STYLE_GRAPH_KEYS: tuple[str, ...] = EVENT_STYLE_KEYS + ANNUAL_STYLE_KEYS
VALID_TIME_DISPLAY_MODES: frozenset[str] = frozenset({"datetime", "24h"})
SCHEMA_PACKAGE = "hydrology_graphs.io"
SCHEMA_FILE = "schemas/style_schema_2_0.json"


def _base_style() -> dict[str, Any]:
    return {
        "font_family": "Yu Gothic UI",
        "font_size": 11,
        "figure_width": 12,
        "figure_height": 6,
        "margin": {"top": 0.08, "right": 0.04, "bottom": 0.12, "left": 0.08},
        "legend": {"enabled": True, "position": "upper right"},
        "dpi": 120,
        "grid": {
            "enabled": True,
            "x_enabled": True,
            "y_enabled": True,
            "color": "#CBD5E1",
            "style": "dashed",
            "width": 0.8,
            "alpha": 0.7,
        },
        "font": {
            "title_size": 14,
            "x_label_size": 12,
            "y_label_size": 12,
            "x_tick_size": 10,
            "y_tick_size": 10,
            "legend_size": 10,
        },
        "export": {"transparent_background": False},
    }


def _style_hyetograph() -> dict[str, Any]:
    style = _base_style()
    style.update(
        {
            "series_color": "#2563EB",
            "series_width": 1.2,
            "series_style": "solid",
            "axis": {
                "x_label": "時刻",
                "y_label": "雨量 (mm/h)",
                "x_label_offset": 0.0,
                "y_label_offset": 0.0,
            },
            "bar_color": "#60A5FA",
            "secondary_series_color": "#1E3A8A",
            "invert_y_axis": True,
            "title": {"template": "{station_name} ハイエトグラフ"},
            "x_axis": {
                "date_format": "%m/%d %H:%M",
                "show_date_labels": True,
                "tick_rotation": 0,
                "tick_interval_hours": 6,
                "tick_hours_of_day": "",
                "label_align": "center",
                "tick_label_pad": 0.0,
                "data_trim_enabled": True,
                "data_trim_start_hours": 0.0,
                "data_trim_end_hours": 0.0,
            },
            "grid": {
                "enabled": True,
                "x_enabled": False,
                "y_enabled": True,
                "color": "#CBD5E1",
                "style": "dashed",
                "width": 0.8,
                "alpha": 0.7,
            },
            "y_axis": {"enabled": True, "number_format": "plain", "tick_count": 6, "max": 80, "tick_label_pad": 0.0},
            "y2_axis": {
                "enabled": True,
                "max": 250,
                "tick_step": 25,
                "number_format": "plain",
                "label": "累積雨量",
                "label_rotation": "270",
                "tick_label_pad": 0.0,
            },
            "bar": {"enabled": True, "width": 0.04, "edge_width": 0.8, "edge_alpha": 0.8},
            "cumulative_line": {"enabled": True, "color": "#1E3A8A", "width": 1.6, "style": "solid"},
            "missing_band": {"enabled": True, "color": "#9CA3AF", "alpha": 0.28},
            "threshold": {
                "label_enabled": True,
                "label_offset": 0.02,
                "label_font_size": 10,
                "zorder": 3,
            },
            "series": {"enabled": True, "zorder": 2},
        }
    )
    return style


def _style_hydro_discharge() -> dict[str, Any]:
    style = _base_style()
    style.update(
        {
            "series_color": "#0F766E",
            "series_width": 1.5,
            "series_style": "solid",
            "axis": {
                "x_label": "時刻",
                "y_label": "流量 (m³/s)",
                "x_label_offset": 0.0,
                "y_label_offset": 0.0,
            },
            "title": {"template": "{station_name} ハイドログラフ（流量）"},
            "x_axis": {
                "date_format": "%m/%d %H:%M",
                "show_date_labels": True,
                "tick_rotation": 0,
                "tick_interval_hours": 6,
                "tick_hours_of_day": "",
                "label_align": "center",
                "tick_label_pad": 0.0,
                "data_trim_enabled": True,
                "data_trim_start_hours": 0.0,
                "data_trim_end_hours": 0.0,
            },
            "y_axis": {"enabled": True, "number_format": "comma", "tick_count": 8, "tick_label_pad": 0.0},
            "missing_band": {"enabled": True, "color": "#9CA3AF", "alpha": 0.28},
            "threshold": {
                "label_enabled": True,
                "label_offset": 0.03,
                "label_font_size": 10,
                "zorder": 3,
            },
            "series": {"enabled": True, "zorder": 2},
        }
    )
    return style


def _style_hydro_water_level() -> dict[str, Any]:
    style = _base_style()
    style.update(
        {
            "series_color": "#7C3AED",
            "series_width": 1.5,
            "series_style": "solid",
            "axis": {
                "x_label": "時刻",
                "y_label": "水位 (m)",
                "x_label_offset": 0.0,
                "y_label_offset": 0.0,
            },
            "title": {"template": "{station_name} ハイドログラフ（水位）"},
            "x_axis": {
                "date_format": "%m/%d %H:%M",
                "show_date_labels": True,
                "tick_rotation": 0,
                "tick_interval_hours": 6,
                "tick_hours_of_day": "",
                "label_align": "center",
                "tick_label_pad": 0.0,
                "data_trim_enabled": True,
                "data_trim_start_hours": 0.0,
                "data_trim_end_hours": 0.0,
            },
            "y_axis": {"enabled": True, "number_format": "plain", "tick_count": 7, "tick_label_pad": 0.0},
            "missing_band": {"enabled": True, "color": "#9CA3AF", "alpha": 0.28},
            "threshold": {
                "label_enabled": True,
                "label_offset": 0.05,
                "label_font_size": 10,
                "zorder": 3,
            },
            "series": {"enabled": True, "zorder": 2},
        }
    )
    return style


def _style_annual_rainfall() -> dict[str, Any]:
    style = _base_style()
    style.update(
        {
            "series_color": "#1D4ED8",
            "series_width": 1.2,
            "series_style": "solid",
            "axis": {"x_label": "年", "y_label": "年最大雨量", "x_label_offset": 0.0, "y_label_offset": 0.0},
            "bar_color": "#60A5FA",
            "title": {"template": "{station_name} 年最大雨量"},
            "x_axis": {"tick_rotation": 0, "label_align": "center", "tick_label_pad": 0.0, "year_tick_step": 1},
            "y_axis": {"enabled": True, "number_format": "plain", "tick_count": 8, "tick_label_pad": 0.0},
            "bar": {"enabled": True, "width": 0.8},
            "series": {"enabled": True, "zorder": 2},
        }
    )
    return style


def _style_annual_discharge() -> dict[str, Any]:
    style = _base_style()
    style.update(
        {
            "series_color": "#0F766E",
            "series_width": 1.2,
            "series_style": "solid",
            "axis": {"x_label": "年", "y_label": "年最大流量", "x_label_offset": 0.0, "y_label_offset": 0.0},
            "bar_color": "#34D399",
            "title": {"template": "{station_name} 年最大流量"},
            "x_axis": {"tick_rotation": 0, "label_align": "center", "tick_label_pad": 0.0, "year_tick_step": 1},
            "y_axis": {"enabled": True, "number_format": "comma", "tick_count": 8, "tick_label_pad": 0.0},
            "bar": {"enabled": True, "width": 0.8},
            "series": {"enabled": True, "zorder": 2},
        }
    )
    return style


def _style_annual_water_level() -> dict[str, Any]:
    style = _base_style()
    style.update(
        {
            "series_color": "#6D28D9",
            "series_width": 1.2,
            "series_style": "solid",
            "axis": {"x_label": "年", "y_label": "年最高水位", "x_label_offset": 0.0, "y_label_offset": 0.0},
            "bar_color": "#A78BFA",
            "title": {"template": "{station_name} 年最高水位"},
            "x_axis": {"tick_rotation": 0, "label_align": "center", "tick_label_pad": 0.0, "year_tick_step": 1},
            "y_axis": {"enabled": True, "number_format": "plain", "tick_count": 8, "tick_label_pad": 0.0},
            "bar": {"enabled": True, "width": 0.8},
            "series": {"enabled": True, "zorder": 2},
        }
    )
    return style


def _default_graph_styles() -> dict[str, dict[str, Any]]:
    return {
        "hyetograph:3day": _style_hyetograph(),
        "hyetograph:5day": _style_hyetograph(),
        "hydrograph_discharge:3day": _style_hydro_discharge(),
        "hydrograph_discharge:5day": _style_hydro_discharge(),
        "hydrograph_water_level:3day": _style_hydro_water_level(),
        "hydrograph_water_level:5day": _style_hydro_water_level(),
        "annual_max_rainfall": _style_annual_rainfall(),
        "annual_max_discharge": _style_annual_discharge(),
        "annual_max_water_level": _style_annual_water_level(),
    }


DEFAULT_STYLE: dict[str, Any] = {
    "schema_version": "2.0",
    "graph_styles": _default_graph_styles(),
    "display": {"time_display_mode": "datetime"},
}


@dataclass(slots=True)
class StyleLoadResult:
    style: dict[str, Any]
    warnings: list[str]

    @property
    def is_valid(self) -> bool:
        return not any(msg.startswith("error:") for msg in self.warnings)


def default_style() -> dict[str, Any]:
    return deepcopy(DEFAULT_STYLE)


def style_key_for_target(graph_type: str, event_window_days: int | None) -> str | None:
    if graph_type in EVENT_GRAPH_TYPES:
        if event_window_days not in (3, 5):
            return None
        return f"{graph_type}:{event_window_days}day"
    if graph_type in GRAPH_TYPES:
        return graph_type
    return None


def load_style(path: str | Path | None = None, *, payload: dict | None = None) -> StyleLoadResult:
    warnings: list[str] = []
    if payload is not None:
        raw = deepcopy(payload)
    elif path and str(path).strip():
        file_path = Path(path)
        if not file_path.exists():
            return StyleLoadResult(style=default_style(), warnings=[f"style_file_not_found: {file_path}"])
        raw = json.loads(file_path.read_text(encoding="utf-8"))
    else:
        raw = default_style()

    style, normalize_warnings = _normalize_style(raw)
    warnings.extend(normalize_warnings)
    return StyleLoadResult(style=style, warnings=warnings)


def save_style(path: str | Path, style: dict) -> None:
    normalized, warnings = _normalize_style(style)
    hard_errors = [warning for warning in warnings if warning.startswith("error:")]
    if hard_errors:
        raise ValueError("; ".join(hard_errors))
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(json.dumps(normalized, ensure_ascii=False, indent=2), encoding="utf-8")


@lru_cache(maxsize=1)
def _load_style_schema() -> dict[str, Any]:
    schema_text = resources.files(SCHEMA_PACKAGE).joinpath(SCHEMA_FILE).read_text(encoding="utf-8")
    return json.loads(schema_text)


@lru_cache(maxsize=1)
def _style_schema_validator() -> Draft202012Validator:
    return Draft202012Validator(_load_style_schema())


def _schema_missing_required_key(message: str) -> str | None:
    matched = re.match(r"^'([^']+)' is a required property$", message)
    if not matched:
        return None
    return matched.group(1)


def _schema_graph_style_validation_error_to_warning(path: list[Any], validator: str, style_key: str) -> str | None:
    if len(path) < 3:
        return None
    suffix = path[2:]
    head = suffix[0]
    if validator == "pattern":
        if head == "series_color":
            return f"error:{style_key}_series_color_must_be_hex"
        if head == "bar_color":
            return f"error:{style_key}_bar_color_must_be_hex"
        if head == "secondary_series_color":
            return f"error:{style_key}_secondary_series_color_must_be_hex"
        if head == "series2" and len(suffix) >= 2 and suffix[1] == "color":
            return f"error:{style_key}_series2_color_must_be_hex"
    if validator == "enum" and head == "series_style":
        return f"error:{style_key}_series_style_invalid"
    if validator == "enum" and head == "series2" and len(suffix) >= 2 and suffix[1] == "style":
        return f"error:{style_key}_series2_style_invalid"
    if validator == "exclusiveMinimum":
        if head in {"font_size", "figure_width", "figure_height", "dpi", "series_width"}:
            return f"error:{style_key}_{head}_must_be_positive"
        if head == "series2" and len(suffix) >= 2 and suffix[1] == "width":
            return f"error:{style_key}_series2_width_must_be_positive"
        if head == "grid" and len(suffix) >= 2 and suffix[1] == "width":
            return f"error:{style_key}_grid_width_must_be_positive"
        if head == "x_axis" and len(suffix) >= 2 and suffix[1] == "tick_interval_hours":
            return f"error:{style_key}_x_axis_tick_interval_hours_must_be_positive"
        if head == "bar" and len(suffix) >= 2 and suffix[1] == "width":
            return f"error:{style_key}_bar_width_must_be_positive"
        if head == "y_axis" and len(suffix) >= 2 and suffix[1] == "tick_step":
            return f"error:{style_key}_y_axis_tick_step_must_be_positive"
        if head == "threshold" and len(suffix) >= 2 and suffix[1] == "label_font_size":
            return f"error:{style_key}_threshold_label_font_size_must_be_positive"
    if validator == "minimum":
        if head == "margin" and len(suffix) >= 2 and suffix[1] in {"top", "right", "bottom", "left"}:
            return f"error:{style_key}_margin_{suffix[1]}_must_be_non_negative"
        if head == "x_axis" and len(suffix) >= 2 and suffix[1] == "range_margin_rate":
            return f"error:{style_key}_x_axis_range_margin_rate_must_be_non_negative"
        if head == "x_axis" and len(suffix) >= 2 and suffix[1] == "data_trim_start_hours":
            return f"error:{style_key}_x_axis_data_trim_start_hours_must_be_non_negative"
        if head == "x_axis" and len(suffix) >= 2 and suffix[1] == "data_trim_end_hours":
            return f"error:{style_key}_x_axis_data_trim_end_hours_must_be_non_negative"
        if head == "x_axis" and len(suffix) >= 2 and suffix[1] == "year_tick_step":
            return f"error:{style_key}_x_axis_year_tick_step_must_be_positive_int"
        if head == "axis" and len(suffix) >= 2 and suffix[1] in {"x_label_offset", "y_label_offset"}:
            return f"error:{style_key}_axis_{suffix[1]}_must_be_number"
        if head == "y_axis" and len(suffix) >= 2 and suffix[1] == "tick_count":
            return f"error:{style_key}_y_axis_tick_count_must_be_positive_int"
    if validator == "type":
        if head == "axis":
            return f"error:{style_key}_axis_invalid"
        if head == "margin":
            return f"error:{style_key}_margin_must_be_object"
        if head == "x_axis" and len(suffix) >= 2 and suffix[1] == "date_boundary_line_enabled":
            return f"error:{style_key}_x_axis_date_boundary_line_enabled_must_be_boolean"
        if head == "x_axis" and len(suffix) >= 2 and suffix[1] == "data_trim_enabled":
            return f"error:{style_key}_x_axis_data_trim_enabled_must_be_boolean"
        if head == "x_axis" and len(suffix) >= 2 and suffix[1] == "show_date_labels":
            return f"error:{style_key}_x_axis_show_date_labels_must_be_boolean"
        if head == "x_axis" and len(suffix) >= 2 and suffix[1] == "date_boundary_line_offset_hours":
            return f"error:{style_key}_x_axis_date_boundary_line_offset_hours_must_be_number"
        if head == "x_axis" and len(suffix) >= 2 and suffix[1] == "tick_hours_of_day":
            return f"error:{style_key}_x_axis_tick_hours_of_day_must_be_string"
        if head == "x_axis" and len(suffix) >= 2 and suffix[1] == "year_tick_step":
            return f"error:{style_key}_x_axis_year_tick_step_must_be_positive_int"
        if head == "x_axis" and len(suffix) >= 2 and suffix[1] == "data_trim_start_hours":
            return f"error:{style_key}_x_axis_data_trim_start_hours_must_be_number"
        if head == "x_axis" and len(suffix) >= 2 and suffix[1] == "data_trim_end_hours":
            return f"error:{style_key}_x_axis_data_trim_end_hours_must_be_number"
        if head == "grid" and len(suffix) >= 2 and suffix[1] == "enabled":
            return f"error:{style_key}_grid_enabled_must_be_boolean"
        if head == "bar" and len(suffix) >= 2 and suffix[1] == "enabled":
            return f"error:{style_key}_bar_enabled_must_be_boolean"
        if head == "y_axis" and len(suffix) >= 2 and suffix[1] == "enabled":
            return f"error:{style_key}_y_axis_enabled_must_be_boolean"
        if head == "series" and len(suffix) >= 2 and suffix[1] == "enabled":
            return f"error:{style_key}_series_enabled_must_be_boolean"
        if head == "series2":
            if len(suffix) >= 2:
                if suffix[1] == "enabled":
                    return f"error:{style_key}_series2_enabled_must_be_boolean"
                if suffix[1] == "use_secondary_y":
                    return f"error:{style_key}_series2_use_secondary_y_must_be_boolean"
            return f"error:{style_key}_series2_invalid"
        if head == "y_axis" and len(suffix) >= 2 and suffix[1] == "tick_count":
            return f"error:{style_key}_y_axis_tick_count_must_be_positive_int"
    return None


def _schema_error_to_warning(error: Any) -> str:
    path = list(error.absolute_path)
    if error.validator == "required":
        missing_key = _schema_missing_required_key(error.message)
        if missing_key is None:
            return "error:style_schema_validation_failed"
        if path == ["graph_styles"]:
            return f"error:missing_graph_style:{missing_key}"
        if len(path) == 2 and path[0] == "graph_styles" and isinstance(path[1], str):
            return f"error:missing_graph_style_keys:{path[1]}:{missing_key}"
    if error.validator == "additionalProperties":
        if path == []:
            return "error:style_root_unknown_property"
        if path == ["graph_styles"]:
            return "error:graph_styles_unknown_property"
    if len(path) >= 2 and path[0] == "graph_styles" and isinstance(path[1], str):
        mapped = _schema_graph_style_validation_error_to_warning(path, error.validator, path[1])
        if mapped is not None:
            return mapped
    return f"error:style_schema_validation_failed:{error.message}"


def _validate_style_contract_with_schema(raw: dict) -> str | None:
    validator = _style_schema_validator()
    errors = sorted(validator.iter_errors(raw), key=lambda err: (len(err.absolute_path), list(err.absolute_path)))
    if not errors:
        return None
    return _schema_error_to_warning(errors[0])


def _drop_optional_none_values_for_schema(raw: dict[str, Any]) -> None:
    """互換のため、任意数値キーの null を検証前に除去する。"""

    graph_styles = raw.get("graph_styles")
    if not isinstance(graph_styles, dict):
        return
    optional_paths = (
        ("x_axis", "range_margin_rate"),
        ("x_axis", "tick_interval_hours"),
        ("x_axis", "tick_hours_of_day"),
        ("x_axis", "year_tick_step"),
        ("x_axis", "date_boundary_line_enabled"),
        ("x_axis", "data_trim_enabled"),
        ("x_axis", "show_date_labels"),
        ("x_axis", "date_boundary_line_offset_hours"),
        ("x_axis", "data_trim_start_hours"),
        ("x_axis", "data_trim_end_hours"),
        ("grid", "enabled"),
        ("bar", "enabled"),
        ("y_axis", "enabled"),
        ("series", "enabled"),
        ("bar", "width"),
        ("bar", "edge_width"),
        ("bar", "edge_alpha"),
        ("grid", "width"),
        ("axis", "x_label_offset"),
        ("axis", "y_label_offset"),
        ("x_axis", "tick_label_pad"),
        ("y_axis", "tick_label_pad"),
        ("y2_axis", "tick_label_pad"),
        ("y2_axis", "label_rotation"),
        ("y_axis", "tick_step"),
        ("y_axis", "max"),
        ("y2_axis", "tick_step"),
        ("y2_axis", "max"),
        ("missing_band", "alpha"),
        ("series2", "enabled"),
        ("series2", "use_secondary_y"),
        ("series2", "width"),
        ("y_axis", "tick_count"),
        ("threshold", "label_font_size"),
    )
    for graph_style in graph_styles.values():
        if not isinstance(graph_style, dict):
            continue
        for parent_key, leaf_key in optional_paths:
            parent = graph_style.get(parent_key)
            if not isinstance(parent, dict):
                continue
            if parent.get(leaf_key, object()) is None:
                parent.pop(leaf_key, None)


def _drop_removed_keys_for_schema(raw: dict[str, Any]) -> None:
    """廃止済みキーを検証前に除去する。"""

    graph_styles = raw.get("graph_styles")
    if not isinstance(graph_styles, dict):
        return
    for graph_style in graph_styles.values():
        if not isinstance(graph_style, dict):
            continue
        graph_style.pop("background_color", None)


def _normalize_style(raw: dict) -> tuple[dict, list[str]]:
    warnings: list[str] = []
    if not isinstance(raw, dict):
        return default_style(), ["error:style_root_must_be_object"]

    schema_version = str(raw.get("schema_version", "")).strip()
    if schema_version != "2.0":
        return default_style(), [f"error:unsupported_schema_version:{schema_version}"]
    if "common" in raw:
        return default_style(), ["error:common_removed_in_schema_2_0"]
    if "variants" in raw:
        return default_style(), ["error:variants_removed_in_schema_2_0"]

    schema_input = deepcopy(raw)
    schema_input["display"] = _normalize_display(schema_input.get("display"), warnings)
    _drop_removed_keys_for_schema(schema_input)
    _drop_optional_none_values_for_schema(schema_input)

    graph_styles_raw = schema_input.get("graph_styles")
    if not isinstance(graph_styles_raw, dict):
        return default_style(), [*warnings, "error:graph_styles_must_be_object"]

    schema_error = _validate_style_contract_with_schema(schema_input)
    if schema_error is not None:
        return default_style(), [*warnings, schema_error]

    merged = default_style()
    _deep_merge(merged, schema_input)
    merged["display"] = schema_input["display"]
    graph_styles = merged["graph_styles"]
    for key in list(graph_styles.keys()):
        if key not in STYLE_GRAPH_KEYS:
            warnings.append(f"unknown_graph_style_key:{key}")
            graph_styles.pop(key, None)
    for key in STYLE_GRAPH_KEYS:
        _normalize_graph_style(graph_styles[key], warnings, style_key=key, raw_graph=graph_styles_raw[key])

    return merged, warnings


def _normalize_display(display: Any, warnings: list[str]) -> dict[str, Any]:
    if not isinstance(display, dict):
        if display is not None:
            warnings.append("warning:display_must_be_object")
        return {"time_display_mode": "datetime"}

    time_display_mode = str(display.get("time_display_mode", "datetime")).strip() or "datetime"
    if time_display_mode not in VALID_TIME_DISPLAY_MODES:
        warnings.append(f"warning:display_time_display_mode_unknown:{time_display_mode}")
        time_display_mode = "datetime"
    return {"time_display_mode": time_display_mode}


def _normalize_graph_style(style: dict, warnings: list[str], *, style_key: str, raw_graph: dict) -> None:
    if not isinstance(style, dict):
        warnings.append("error:graph_style_must_be_object")
        return
    if "title_template" in raw_graph:
        if "title" not in style or not isinstance(style["title"], dict):
            style["title"] = {"template": raw_graph["title_template"]}
        elif "template" not in raw_graph.get("title", {}):
            style["title"]["template"] = raw_graph["title_template"]
    style.pop("title_template", None)
    if "title" not in style or not isinstance(style["title"], dict):
        style["title"] = {"template": "{station_name}"}
    style["title"].setdefault("template", "{station_name}")
    if "axis" not in style or not isinstance(style["axis"], dict):
        style["axis"] = {"x_label": "", "y_label": ""}
    axis = cast(dict[str, Any], style["axis"])
    axis.setdefault("x_label", "")
    axis.setdefault("y_label", "")
    axis.setdefault("x_label_offset", 0.0)
    axis.setdefault("y_label_offset", 0.0)
    if "x_axis" not in style or not isinstance(style["x_axis"], dict):
        style["x_axis"] = {}
    style["x_axis"].setdefault("range_margin_rate", 0)
    style["x_axis"].setdefault("date_boundary_line_enabled", False)
    style["x_axis"].setdefault("data_trim_enabled", True)
    style["x_axis"].setdefault("show_date_labels", True)
    style["x_axis"].setdefault("year_tick_step", 1)
    style["x_axis"].setdefault("date_boundary_line_offset_hours", 0.0)
    style["x_axis"].setdefault("tick_hours_of_day", "")
    style["x_axis"].setdefault("data_trim_start_hours", 0.0)
    style["x_axis"].setdefault("data_trim_end_hours", 0.0)
    style["x_axis"].setdefault("tick_label_pad", 0.0)
    if "grid" not in style or not isinstance(style["grid"], dict):
        style["grid"] = {}
    grid_enabled_fallback = (
        bool(raw_graph.get("grid", {}).get("enabled", True)) if isinstance(raw_graph.get("grid"), dict) else True
    )
    style["grid"].setdefault("x_enabled", grid_enabled_fallback)
    style["grid"].setdefault("y_enabled", grid_enabled_fallback)
    style["grid"].setdefault("width", 0.8)
    style["grid"].setdefault("style", "dashed")
    style["grid"].setdefault("color", "#CBD5E1")
    style["grid"].setdefault("alpha", 0.7)
    if "enabled" not in style["grid"]:
        style["grid"]["enabled"] = grid_enabled_fallback
    if "bar" not in style or not isinstance(style["bar"], dict):
        style["bar"] = {}
    style["bar"].setdefault("enabled", True)
    if "series" not in style or not isinstance(style["series"], dict):
        style["series"] = {}
    style["series"].setdefault("enabled", True)

    is_hydro = any(style_key.startswith(prefix) for prefix in (GRAPH_HYDRO_DISCHARGE, GRAPH_HYDRO_WATER_LEVEL))
    if is_hydro:
        if "series2" not in style or not isinstance(style["series2"], dict):
            style["series2"] = {}
        style["series2"].setdefault("enabled", False)
        style["series2"].setdefault("color", "#F59E0B")
        style["series2"].setdefault("width", 1.5)
        style["series2"].setdefault("style", "dashed")
        style["series2"].setdefault("use_secondary_y", False)
    else:
        style.pop("series2", None)

    if "y_axis" not in style or not isinstance(style["y_axis"], dict):
        style["y_axis"] = {}
    style["y_axis"].setdefault("enabled", True)
    style["y_axis"].setdefault("tick_label_pad", 0.0)
    if "y2_axis" not in style or not isinstance(style["y2_axis"], dict):
        style["y2_axis"] = {}
    style["y2_axis"].setdefault("enabled", True)
    style["y2_axis"].setdefault("label", "累積雨量")
    style["y2_axis"].setdefault("label_rotation", "270")
    rotation_raw = style["y2_axis"].get("label_rotation")
    if isinstance(rotation_raw, (int, float)) and float(rotation_raw).is_integer():
        rotation_raw = str(int(rotation_raw))
    else:
        rotation_raw = str(rotation_raw).strip()
    if rotation_raw not in {"0", "90", "180", "270"}:
        rotation_raw = "270"
    style["y2_axis"]["label_rotation"] = rotation_raw
    style["y2_axis"].setdefault("label_offset", 0.0)
    style["y2_axis"].setdefault("number_format", style["y_axis"].get("number_format", "plain"))
    style["y2_axis"].setdefault("tick_label_pad", 0.0)
    if "missing_band" not in style or not isinstance(style["missing_band"], dict):
        style["missing_band"] = {}
    style["missing_band"].setdefault("enabled", False)
    style["missing_band"].setdefault("color", "#9CA3AF")
    style["missing_band"].setdefault("alpha", 0.28)
    if "font" not in style or not isinstance(style["font"], dict):
        style["font"] = {}
    raw_font_size = raw_graph.get("font_size")
    fallback_font_size = (
        float(raw_font_size) if isinstance(raw_font_size, (int, float)) else float(style.get("font_size", 11))
    )
    style["font"].setdefault("title_size", max(fallback_font_size + 3, 1.0))
    style["font"].setdefault("x_label_size", max(fallback_font_size + 1, 1.0))
    style["font"].setdefault("y_label_size", max(fallback_font_size + 1, 1.0))
    style["font"].setdefault("x_tick_size", max(fallback_font_size - 1, 1.0))
    style["font"].setdefault("y_tick_size", max(fallback_font_size - 1, 1.0))
    style["font"].setdefault("legend_size", max(fallback_font_size - 1, 1.0))


def _deep_merge(base: dict, incoming: dict) -> None:
    for key, value in incoming.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value
