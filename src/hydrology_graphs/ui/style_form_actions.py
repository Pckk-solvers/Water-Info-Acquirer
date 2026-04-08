from __future__ import annotations

import tkinter as tk
from typing import Any

from .style_payload import delete_nested_value, nested_value, set_nested_value


def set_control_var(control: dict[str, Any], value: Any) -> None:
    """型に応じて制御変数へ値を反映する。"""

    kind = control["kind"]
    var: tk.Variable = control["var"]
    if kind == "bool":
        var.set(bool(value))
        return
    if value is None:
        var.set("")
        return
    var.set(str(value))


def apply_group_toggle_states(app) -> None:
    """グループトグルに応じて編集可否を切り替える。"""

    toggles: dict[str, bool] = {}
    for control in app._style_graph_controls:
        if not bool(control.get("is_group_toggle")):
            continue
        path = str(control.get("path", "")).strip()
        toggles[path] = bool(control["var"].get())

    for control in app._style_graph_controls:
        group_path = str(control.get("group_toggle_path", "")).strip()
        if not group_path:
            continue
        enabled = toggles.get(group_path, True)
        widget = control.get("widget")
        if widget is None:
            continue
        kind = str(control.get("kind", "str"))
        if kind == "choice":
            widget.configure(state="readonly" if enabled else "disabled")
        elif kind == "button":
            widget.configure(state="normal" if enabled else "disabled")
        elif kind == "bool":
            if enabled:
                widget.state(["!disabled"])
            else:
                widget.state(["disabled"])
        else:
            widget.configure(state="normal" if enabled else "disabled")


def coerce_control_value(control: dict[str, Any], current_value: Any, *, empty_numeric: object) -> tuple[Any, str | None]:
    """フォーム入力値を設定型へ変換する。"""

    kind = control["kind"]
    label = control["label"]
    var: tk.Variable = control["var"]
    if kind == "bool":
        return bool(var.get()), None
    text = str(var.get()).strip()
    if kind == "str" or kind == "choice":
        return text, None
    if text == "":
        if kind in {"int", "float"}:
            return empty_numeric, None
        return current_value, None
    if kind == "int":
        try:
            parsed = float(text)
        except ValueError:
            return current_value, f"スタイル入力エラー: {label} は整数を入力してください。"
        if not parsed.is_integer():
            return current_value, f"スタイル入力エラー: {label} は整数を入力してください。"
        return int(parsed), None
    if kind == "float":
        try:
            return float(text), None
        except ValueError:
            return current_value, f"スタイル入力エラー: {label} は数値を入力してください。"
    return text, None


def apply_style_form_values(app, *, empty_numeric: object, valid_time_display_modes: set[str] | frozenset[str]) -> bool:
    """フォームの値を payload に反映する。"""

    graph_key = app._current_style_graph_key()
    graph_styles = app._style_payload.setdefault("graph_styles", {})
    graph_style = graph_styles.get(graph_key)
    if not isinstance(graph_style, dict):
        graph_style = {}
        graph_styles[graph_key] = graph_style
    for control in app._style_graph_controls:
        path = str(control.get("path", "")).strip()
        if not path or path.startswith("__palette__"):
            continue
        current_value = nested_value(graph_style, control["path"], None)
        value, error = coerce_control_value(control, current_value, empty_numeric=empty_numeric)
        if error:
            app.preview_message.set(error)
            return False
        if value is empty_numeric:
            delete_nested_value(graph_style, control["path"])
            continue
        set_nested_value(graph_style, control["path"], value)
    time_display_mode = str(app.time_display_mode.get()).strip() or "datetime"
    if time_display_mode not in valid_time_display_modes:
        time_display_mode = "datetime"
    set_nested_value(app._style_payload, "display.time_display_mode", time_display_mode)
    return True
