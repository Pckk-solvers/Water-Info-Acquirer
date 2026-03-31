from __future__ import annotations

import json
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..domain.constants import GRAPH_TYPES

"""描画スタイル JSON の読込・保存・正規化。

GUI の編集内容をそのまま保存せず、保存前に契約へ寄せる。
"""


DEFAULT_STYLE: dict[str, Any] = {
    "schema_version": "1.0",
    "common": {
        "font_family": "Yu Gothic UI",
        "font_size": 11,
        "figure_width": 12,
        "figure_height": 6,
        "margin": {"top": 0.08, "right": 0.04, "bottom": 0.12, "left": 0.08},
        "legend": {"enabled": True, "position": "upper right"},
        "aspect_mode": "fixed",
        "dpi": 120,
        "grid": {"enabled": True, "color": "#CBD5E1", "style": "--", "alpha": 0.7},
        "background_color": "#FFFFFF",
        "padding": {"outer": {"top": 0.08, "right": 0.04, "bottom": 0.12, "left": 0.08}},
        "font": {"title_size": 14, "label_size": 12, "tick_size": 10},
        "export": {"transparent_background": False},
    },
    "graph_styles": {
        "hyetograph": {
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
        },
        "hydrograph_discharge": {
            "series_color": "#0F766E",
            "series_width": 1.5,
            "series_style": "solid",
            "axis": {"x_label": "時刻", "y_label": "流量 (m3/s)"},
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
        },
        "hydrograph_water_level": {
            "series_color": "#7C3AED",
            "series_width": 1.5,
            "series_style": "solid",
            "axis": {"x_label": "時刻", "y_label": "水位 (m)"},
            "title": {"template": "{station_name} 水位波形"},
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
        },
        "annual_max_rainfall": {
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
        },
        "annual_max_discharge": {
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
        },
        "annual_max_water_level": {
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
        },
    },
}


@dataclass(slots=True)
class StyleLoadResult:
    """正規化後スタイルと警告のセット。"""

    style: dict[str, Any]
    warnings: list[str]

    @property
    def is_valid(self) -> bool:
        """致命的なエラーがないかを返す。"""

        return not any(msg.startswith("error:") for msg in self.warnings)


def default_style() -> dict[str, Any]:
    """既定のスタイル設定を複製して返す。"""

    return deepcopy(DEFAULT_STYLE)


def load_style(path: str | Path | None = None, *, payload: dict | None = None) -> StyleLoadResult:
    """ファイルまたはメモリ上の payload からスタイルを読み込む。"""

    warnings: list[str] = []
    if payload is not None:
        # GUI の編集中 JSON をそのまま渡す経路。
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
    """スタイルを正規化した上で JSON として保存する。"""

    normalized, warnings = _normalize_style(style)
    hard_errors = [warning for warning in warnings if warning.startswith("error:")]
    if hard_errors:
        raise ValueError("; ".join(hard_errors))
    file_path = Path(path)
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.write_text(json.dumps(normalized, ensure_ascii=False, indent=2), encoding="utf-8")


def _normalize_style(raw: dict) -> tuple[dict, list[str]]:
    """入力されたスタイルを既定構造へ寄せる。"""

    warnings: list[str] = []
    if not isinstance(raw, dict):
        return default_style(), ["error:style_root_must_be_object"]

    # schema_version は保存互換の基準なので、ここで必ず確認する。
    schema_version = str(raw.get("schema_version", "")).strip()
    if schema_version != "1.0":
        return default_style(), [f"error:unsupported_schema_version:{schema_version}"]

    common_raw = raw.get("common")
    graph_styles_raw = raw.get("graph_styles")
    if not isinstance(common_raw, dict):
        return default_style(), ["error:common_must_be_object"]
    if not isinstance(graph_styles_raw, dict):
        return default_style(), ["error:graph_styles_must_be_object"]

    required_common_keys = ("font_family", "font_size", "figure_width", "figure_height", "margin", "legend")
    missing_common = [key for key in required_common_keys if key not in common_raw]
    if missing_common:
        return default_style(), [f"error:missing_common_keys:{', '.join(missing_common)}"]

    required_graph_keys = ("series_color", "series_width", "series_style", "axis")
    for key in GRAPH_TYPES:
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
        if key not in GRAPH_TYPES:
            # 未知キーは警告に落とし、保存形からは外す。
            warnings.append(f"unknown_graph_style_key:{key}")
            graph_styles.pop(key, None)
    for key in GRAPH_TYPES:
        if key not in graph_styles:
            # 個別設定が欠けていても、既定値で補える範囲は補う。
            warnings.append(f"missing_graph_style_key:{key}")
            graph_styles[key] = deepcopy(DEFAULT_STYLE["graph_styles"][key])
        _normalize_graph_style(graph_styles[key], warnings, raw_graph=graph_styles_raw[key])
        _validate_graph_style(graph_styles[key], key, warnings)

    _validate_common_block(merged["common"], warnings)

    return merged, warnings


def _normalize_graph_style(style: dict, warnings: list[str], *, raw_graph: dict) -> None:
    """個別グラフ設定の古いキーや不足キーを補う。"""

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


def _validate_common_block(common: dict, warnings: list[str]) -> None:
    """共通設定ブロックの値を検証する。"""

    for key in ("font_size", "figure_width", "figure_height"):
        value = common.get(key)
        if not _is_positive_number(value):
            warnings.append(f"error:common_{key}_must_be_positive")
    margin = common.get("margin", {})
    if isinstance(margin, dict):
        for key in ("top", "right", "bottom", "left"):
            if not _is_non_negative_number(margin.get(key)):
                warnings.append(f"error:common_margin_{key}_must_be_non_negative")
    else:
        warnings.append("error:common_margin_must_be_object")


def _validate_graph_style(style: dict, graph_type: str, warnings: list[str]) -> None:
    """個別グラフ設定の必須項目と値の妥当性を確認する。"""

    if not _is_hex_color(style.get("series_color")):
        warnings.append(f"error:{graph_type}_series_color_must_be_hex")
    if not _is_positive_number(style.get("series_width")):
        warnings.append(f"error:{graph_type}_series_width_must_be_positive")
    if style.get("series_style") not in {"solid", "dashed", "dotted"}:
        warnings.append(f"error:{graph_type}_series_style_invalid")

    axis = style.get("axis", {})
    if not isinstance(axis, dict) or not axis.get("x_label") or not axis.get("y_label"):
        warnings.append(f"error:{graph_type}_axis_invalid")

    if "bar_color" in style and style["bar_color"] is not None and not _is_hex_color(style["bar_color"]):
        warnings.append(f"error:{graph_type}_bar_color_must_be_hex")
    if (
        "secondary_series_color" in style
        and style["secondary_series_color"] is not None
        and not _is_hex_color(style["secondary_series_color"])
    ):
        warnings.append(f"error:{graph_type}_secondary_series_color_must_be_hex")

    x_axis = style.get("x_axis", {})
    if isinstance(x_axis, dict) and x_axis.get("tick_interval_hours") is not None:
        if not _is_positive_number(x_axis.get("tick_interval_hours")):
            warnings.append(f"error:{graph_type}_x_axis_tick_interval_hours_must_be_positive")
    bar = style.get("bar", {})
    if isinstance(bar, dict) and bar.get("width") is not None and not _is_positive_number(bar.get("width")):
        warnings.append(f"error:{graph_type}_bar_width_must_be_positive")
    y_axis = style.get("y_axis", {})
    if isinstance(y_axis, dict):
        if y_axis.get("tick_step") is not None and not _is_positive_number(y_axis.get("tick_step")):
            warnings.append(f"error:{graph_type}_y_axis_tick_step_must_be_positive")
        if y_axis.get("tick_count") is not None and (not isinstance(y_axis.get("tick_count"), int) or y_axis.get("tick_count") <= 0):
            warnings.append(f"error:{graph_type}_y_axis_tick_count_must_be_positive_int")
    threshold = style.get("threshold", {})
    if isinstance(threshold, dict) and threshold.get("label_font_size") is not None:
        if not _is_positive_number(threshold.get("label_font_size")):
            warnings.append(f"error:{graph_type}_threshold_label_font_size_must_be_positive")


def _is_hex_color(value: Any) -> bool:
    """16進カラー文字列かを判定する。"""

    if not isinstance(value, str) or not value.startswith("#"):
        return False
    hex_part = value[1:]
    return len(hex_part) in (6, 8) and all(ch in "0123456789ABCDEFabcdef" for ch in hex_part)


def _is_positive_number(value: Any) -> bool:
    """正の数かを判定する。"""

    try:
        return float(value) > 0
    except Exception:  # noqa: BLE001
        return False


def _is_non_negative_number(value: Any) -> bool:
    """0以上の数かを判定する。"""

    try:
        return float(value) >= 0
    except Exception:  # noqa: BLE001
        return False


def _deep_merge(base: dict, incoming: dict) -> None:
    """辞書を深くマージする。"""

    for key, value in incoming.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value
