from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..domain.constants import EVENT_GRAPH_TYPES, GRAPH_TYPES

"""描画スタイル JSON の読込・保存・正規化。"""


EVENT_STYLE_KEYS: tuple[str, ...] = tuple(f"{graph_type}:{days}day" for graph_type in EVENT_GRAPH_TYPES for days in (3, 5))
ANNUAL_STYLE_KEYS: tuple[str, ...] = tuple(
    graph_type for graph_type in GRAPH_TYPES if graph_type not in EVENT_GRAPH_TYPES
)
STYLE_GRAPH_KEYS: tuple[str, ...] = EVENT_STYLE_KEYS + ANNUAL_STYLE_KEYS


def _base_style() -> dict[str, Any]:
    return {
        "font_family": "Yu Gothic UI",
        "font_size": 11,
        "figure_width": 12,
        "figure_height": 6,
        "margin": {"top": 0.08, "right": 0.04, "bottom": 0.12, "left": 0.08},
        "legend": {"enabled": True, "position": "upper right"},
        "dpi": 120,
        "grid": {"enabled": True, "color": "#CBD5E1", "style": "--", "alpha": 0.7},
        "background_color": "#FFFFFF",
        "font": {"title_size": 14, "label_size": 12, "tick_size": 10},
        "export": {"transparent_background": False},
    }


def _style_hyetograph() -> dict[str, Any]:
    style = _base_style()
    style.update(
        {
            "series_color": "#2563EB",
            "series_width": 1.2,
            "series_style": "solid",
            "axis": {"x_label": "時刻", "y_label": "雨量 (mm/h)"},
            "bar_color": "#60A5FA",
            "secondary_series_color": "#1E3A8A",
            "invert_y_axis": True,
            "title": {"template": "{station_name} ハイエトグラフ"},
            "x_axis": {
                "date_format": "%m/%d %H:%M",
                "tick_rotation": 45,
                "tick_interval_hours": 6,
                "label_align": "center",
            },
            "y_axis": {"number_format": "plain", "tick_count": 6},
            "bar": {"width": 0.8},
            "threshold": {
                "label_enabled": True,
                "label_offset": 0.02,
                "label_font_size": 10,
                "zorder": 3,
            },
            "series": {"zorder": 2},
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
            "axis": {"x_label": "時刻", "y_label": "流量 (m³/s)"},
            "title": {"template": "{station_name} ハイドログラフ（流量）"},
            "x_axis": {
                "date_format": "%m/%d %H:%M",
                "tick_rotation": 45,
                "tick_interval_hours": 6,
                "label_align": "center",
            },
            "y_axis": {"number_format": "comma", "tick_count": 8},
            "threshold": {
                "label_enabled": True,
                "label_offset": 0.03,
                "label_font_size": 10,
                "zorder": 3,
            },
            "series": {"zorder": 2},
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
            "axis": {"x_label": "時刻", "y_label": "水位 (m)"},
            "title": {"template": "{station_name} ハイドログラフ（水位）"},
            "x_axis": {
                "date_format": "%m/%d %H:%M",
                "tick_rotation": 45,
                "tick_interval_hours": 6,
                "label_align": "center",
            },
            "y_axis": {"number_format": "plain", "tick_count": 7},
            "threshold": {
                "label_enabled": True,
                "label_offset": 0.05,
                "label_font_size": 10,
                "zorder": 3,
            },
            "series": {"zorder": 2},
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
            "axis": {"x_label": "年", "y_label": "年最大雨量"},
            "bar_color": "#60A5FA",
            "title": {"template": "{station_name} 年最大雨量"},
            "x_axis": {"tick_rotation": 90, "label_align": "center"},
            "y_axis": {"number_format": "plain", "tick_count": 8},
            "bar": {"width": 0.8},
            "series": {"zorder": 2},
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
            "axis": {"x_label": "年", "y_label": "年最大流量"},
            "bar_color": "#34D399",
            "title": {"template": "{station_name} 年最大流量"},
            "x_axis": {"tick_rotation": 90, "label_align": "center"},
            "y_axis": {"number_format": "comma", "tick_count": 8},
            "bar": {"width": 0.8},
            "series": {"zorder": 2},
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
            "axis": {"x_label": "年", "y_label": "年最高水位"},
            "bar_color": "#A78BFA",
            "title": {"template": "{station_name} 年最高水位"},
            "x_axis": {"tick_rotation": 90, "label_align": "center"},
            "y_axis": {"number_format": "plain", "tick_count": 8},
            "bar": {"width": 0.8},
            "series": {"zorder": 2},
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

    graph_styles_raw = raw.get("graph_styles")
    if not isinstance(graph_styles_raw, dict):
        return default_style(), ["error:graph_styles_must_be_object"]

    required_graph_keys = ("series_color", "series_width", "series_style", "axis")
    for key in STYLE_GRAPH_KEYS:
        raw_graph = graph_styles_raw.get(key)
        if not isinstance(raw_graph, dict):
            return default_style(), [f"error:missing_graph_style:{key}"]
        missing_graph = [item for item in required_graph_keys if item not in raw_graph]
        if missing_graph:
            return default_style(), [f"error:missing_graph_style_keys:{key}:{', '.join(missing_graph)}"]

    merged = default_style()
    _deep_merge(merged, raw)
    graph_styles = merged["graph_styles"]
    for key in list(graph_styles.keys()):
        if key not in STYLE_GRAPH_KEYS:
            warnings.append(f"unknown_graph_style_key:{key}")
            graph_styles.pop(key, None)
    for key in STYLE_GRAPH_KEYS:
        _normalize_graph_style(graph_styles[key], warnings, raw_graph=graph_styles_raw[key])
        _validate_graph_style(graph_styles[key], key, warnings)

    return merged, warnings


def _normalize_graph_style(style: dict, warnings: list[str], *, raw_graph: dict) -> None:
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
    style["axis"].setdefault("x_label", "")
    style["axis"].setdefault("y_label", "")


def _validate_graph_style(style: dict, style_key: str, warnings: list[str]) -> None:
    if not _is_hex_color(style.get("series_color")):
        warnings.append(f"error:{style_key}_series_color_must_be_hex")
    if not _is_positive_number(style.get("series_width")):
        warnings.append(f"error:{style_key}_series_width_must_be_positive")
    if style.get("series_style") not in {"solid", "dashed", "dotted", "dashdot"}:
        warnings.append(f"error:{style_key}_series_style_invalid")
    for key in ("font_size", "figure_width", "figure_height", "dpi"):
        if key in style and style[key] is not None and not _is_positive_number(style[key]):
            warnings.append(f"error:{style_key}_{key}_must_be_positive")

    margin = style.get("margin", {})
    if not isinstance(margin, dict):
        warnings.append(f"error:{style_key}_margin_must_be_object")
    else:
        for key in ("top", "right", "bottom", "left"):
            if not _is_non_negative_number(margin.get(key)):
                warnings.append(f"error:{style_key}_margin_{key}_must_be_non_negative")

    axis = style.get("axis", {})
    if not isinstance(axis, dict):
        warnings.append(f"error:{style_key}_axis_invalid")

    if "background_color" in style and not _is_hex_color(style.get("background_color")):
        warnings.append(f"error:{style_key}_background_color_must_be_hex")
    if "bar_color" in style and style["bar_color"] is not None and not _is_hex_color(style["bar_color"]):
        warnings.append(f"error:{style_key}_bar_color_must_be_hex")
    if (
        "secondary_series_color" in style
        and style["secondary_series_color"] is not None
        and not _is_hex_color(style["secondary_series_color"])
    ):
        warnings.append(f"error:{style_key}_secondary_series_color_must_be_hex")

    x_axis = style.get("x_axis", {})
    if isinstance(x_axis, dict) and x_axis.get("tick_interval_hours") is not None:
        if not _is_positive_number(x_axis.get("tick_interval_hours")):
            warnings.append(f"error:{style_key}_x_axis_tick_interval_hours_must_be_positive")
    bar = style.get("bar", {})
    if isinstance(bar, dict) and bar.get("width") is not None and not _is_positive_number(bar.get("width")):
        warnings.append(f"error:{style_key}_bar_width_must_be_positive")
    y_axis = style.get("y_axis", {})
    if isinstance(y_axis, dict):
        if y_axis.get("tick_step") is not None and not _is_positive_number(y_axis.get("tick_step")):
            warnings.append(f"error:{style_key}_y_axis_tick_step_must_be_positive")
        if y_axis.get("tick_count") is not None and (not isinstance(y_axis.get("tick_count"), int) or y_axis.get("tick_count") <= 0):
            warnings.append(f"error:{style_key}_y_axis_tick_count_must_be_positive_int")
    threshold = style.get("threshold", {})
    if isinstance(threshold, dict) and threshold.get("label_font_size") is not None:
        if not _is_positive_number(threshold.get("label_font_size")):
            warnings.append(f"error:{style_key}_threshold_label_font_size_must_be_positive")


def _is_hex_color(value: Any) -> bool:
    if not isinstance(value, str) or not value.startswith("#"):
        return False
    hex_part = value[1:]
    return len(hex_part) in (6, 8) and all(ch in "0123456789ABCDEFabcdef" for ch in hex_part)


def _is_positive_number(value: Any) -> bool:
    try:
        return float(value) > 0
    except Exception:  # noqa: BLE001
        return False


def _is_non_negative_number(value: Any) -> bool:
    try:
        return float(value) >= 0
    except Exception:  # noqa: BLE001
        return False


def _deep_merge(base: dict, incoming: dict) -> None:
    for key, value in incoming.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value
