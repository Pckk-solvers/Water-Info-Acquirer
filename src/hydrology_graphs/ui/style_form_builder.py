from __future__ import annotations

import tkinter as tk
import tkinter.font as tkfont
from tkinter import ttk
from typing import Any

from .style_payload import nested_value

_VALUE_LABEL_COL_MINSIZE = 62
_VALUE_INPUT_COL_MINSIZE = 88
STYLE_FORM_TOGGLE_COLUMN_MINSIZE = 44
STYLE_FORM_TOGGLE_PADX = (6, 0)
STYLE_FORM_LABEL_PADX = (1, 6)
STYLE_FORM_VALUE_CONTAINER_PADX = (6, 6)


def create_style_control(
    app,
    parent: tk.Misc,
    *,
    row: int,
    field: dict[str, Any],
) -> dict[str, Any]:
    """スタイルフォームの 1 項目を構築して制御情報を返す（3列固定）。"""

    kind = str(field.get("kind", "str"))
    label = str(field.get("label", ""))
    path = str(field.get("path", ""))
    label_widget = ttk.Label(parent, text=label)
    label_widget.grid(row=row, column=1, sticky="w", padx=STYLE_FORM_LABEL_PADX, pady=3)

    if kind == "bool":
        var: tk.Variable = tk.BooleanVar(value=False)
        widget = ttk.Checkbutton(parent, text="", width=1, variable=var, command=app._on_style_form_commit)
        widget.grid(row=row, column=0, sticky="w", padx=STYLE_FORM_TOGGLE_PADX, pady=3)
        ttk.Frame(parent, width=1).grid(row=row, column=2, sticky="ew", padx=6, pady=3)
    elif kind == "choice":
        ttk.Frame(parent, width=1).grid(row=row, column=0, sticky="w", padx=STYLE_FORM_TOGGLE_PADX, pady=3)
        var = tk.StringVar(value="")
        widget = ttk.Combobox(
            parent,
            textvariable=var,
            state="readonly",
            values=tuple(field.get("values") or ()),
            width=24,
        )
        widget.grid(row=row, column=2, sticky="ew", padx=STYLE_FORM_VALUE_CONTAINER_PADX, pady=3)
        widget.bind("<<ComboboxSelected>>", app._on_style_form_commit_event)
        widget.bind("<Return>", app._on_style_form_commit_event)
    else:
        ttk.Frame(parent, width=1).grid(row=row, column=0, sticky="w", padx=STYLE_FORM_TOGGLE_PADX, pady=3)
        var = tk.StringVar(value="")
        widget = ttk.Entry(parent, textvariable=var)
        widget.grid(row=row, column=2, sticky="ew", padx=STYLE_FORM_VALUE_CONTAINER_PADX, pady=3)
        widget.bind("<Return>", app._on_style_form_commit_event)

    return {
        "path": path,
        "label": label,
        "kind": kind,
        "var": var,
        "label_widget": label_widget,
        "widget": widget,
        "tooltip": str(field.get("tooltip", "")).strip(),
    }


def create_compact_input_control(
    app,
    *,
    container: ttk.Frame,
    field: dict[str, Any],
    row_label: str,
    label_widget: tk.Widget,
    group_toggle_path: str | None,
    grid_row: int,
    widget_col: int,
    is_last: bool,
    is_single_full_width: bool,
    label_prefix: str,
    columnspan: int = 1,
) -> dict[str, Any]:
    """compact行の入力ウィジェットを1つ生成して配置する。"""

    kind = str(field.get("kind", "str"))
    input_width = int(field.get("width", 10))
    var: tk.Variable
    if kind == "bool":
        var = tk.BooleanVar(value=False)
        widget = ttk.Checkbutton(container, text="", width=1, variable=var, command=app._on_style_form_commit)
    elif kind == "choice":
        var = tk.StringVar(value="")
        widget = ttk.Combobox(
            container,
            textvariable=var,
            state="readonly",
            values=tuple(field.get("values") or ()),
            width=input_width if not is_single_full_width else 24,
        )
        widget.bind("<<ComboboxSelected>>", app._on_style_form_commit_event)
        widget.bind("<Return>", app._on_style_form_commit_event)
    else:
        var = tk.StringVar(value="")
        widget = ttk.Entry(container, textvariable=var, width=input_width if not is_single_full_width else 24)
        widget.bind("<Return>", app._on_style_form_commit_event)

    pad_right = 0 if is_last else 8
    if kind in {"str", "int", "float", "choice"} and (is_last or is_single_full_width):
        container.columnconfigure(widget_col, weight=1)
    sticky = "ew" if kind in {"str", "int", "float", "choice"} else "w"
    widget.grid(row=grid_row, column=widget_col, columnspan=columnspan, sticky=sticky, padx=(0, pad_right))

    return {
        "path": str(field.get("path", "")),
        "label": label_prefix or row_label,
        "kind": kind,
        "var": var,
        "label_widget": label_widget,
        "widget": widget,
        "tooltip": str(field.get("tooltip", "")).strip(),
        "group_toggle_path": group_toggle_path,
    }


def create_compact_style_row(
    app,
    parent: tk.Misc,
    *,
    row: int,
    row_label: str,
    toggle: dict[str, Any] | None = None,
    values: list[dict[str, Any]] | None = None,
    detail_values: list[dict[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], int]:
    """3列固定の行を構築する（必要なら詳細2行目も使う）。"""

    controls: list[dict[str, Any]] = []
    rows_used = 1
    label_widget = ttk.Label(parent, text=row_label)
    label_widget.grid(row=row, column=1, sticky="w", padx=STYLE_FORM_LABEL_PADX, pady=3)
    toggle_path: str | None = None
    if isinstance(toggle, dict) and str(toggle.get("path", "")).strip():
        toggle_path = str(toggle.get("path", "")).strip()
        var: tk.Variable = tk.BooleanVar(value=False)
        widget = ttk.Checkbutton(parent, text="", width=1, variable=var, command=app._on_style_form_commit)
        widget.grid(row=row, column=0, sticky="w", padx=STYLE_FORM_TOGGLE_PADX, pady=3)
        controls.append(
            {
                "path": toggle_path,
                "label": f"{row_label}:toggle",
                "kind": "bool",
                "var": var,
                "label_widget": label_widget,
                "widget": widget,
                "tooltip": str(toggle.get("tooltip", "")).strip(),
                "is_group_toggle": True,
            }
        )
    else:
        ttk.Frame(parent, width=1).grid(row=row, column=0, sticky="w", padx=STYLE_FORM_TOGGLE_PADX, pady=3)

    value_defs = values or []
    if value_defs:
        value_frame = ttk.Frame(parent)
        value_frame.grid(row=row, column=2, sticky="ew", padx=STYLE_FORM_VALUE_CONTAINER_PADX, pady=3)
        if len(value_defs) <= 2:
            _configure_two_slot_value_columns(value_frame)
            for i, field in enumerate(value_defs):
                caption = str(field.get("label", "")).strip()
                label_col = i * 2
                input_col = label_col + 1
                if caption:
                    ttk.Label(value_frame, text=caption).grid(row=0, column=label_col, sticky="w", padx=(0, 4))
                else:
                    ttk.Frame(value_frame, width=1).grid(row=0, column=label_col, sticky="w")
                is_single_full_width = len(value_defs) == 1 and caption == ""
                controls.append(
                    create_compact_input_control(
                        app,
                        container=value_frame,
                        field=field,
                        row_label=row_label,
                        label_widget=label_widget,
                        group_toggle_path=toggle_path,
                        grid_row=0,
                        widget_col=input_col if not is_single_full_width else 0,
                        columnspan=4 if is_single_full_width else 1,
                        is_last=(i == len(value_defs) - 1),
                        is_single_full_width=is_single_full_width,
                        label_prefix=f"{row_label}:{field.get('label', '')}",
                    )
                )
        else:
            for i, field in enumerate(value_defs):
                caption = str(field.get("label", "")).strip()
                base_col = i * 2
                if caption:
                    ttk.Label(value_frame, text=caption).grid(row=0, column=base_col, sticky="w", padx=(0, 2))
                    widget_col = base_col + 1
                else:
                    widget_col = base_col
                is_single_full_width = len(value_defs) == 1 and caption == ""
                controls.append(
                    create_compact_input_control(
                        app,
                        container=value_frame,
                        field=field,
                        row_label=row_label,
                        label_widget=label_widget,
                        group_toggle_path=toggle_path,
                        grid_row=0,
                        widget_col=widget_col,
                        is_last=(i == len(value_defs) - 1),
                        is_single_full_width=is_single_full_width,
                        label_prefix=f"{row_label}:{field.get('label', '')}",
                    )
                )

    detail_defs = detail_values or []
    if detail_defs:
        rows_used += 1
        detail_label = ttk.Label(parent, text="└ 詳細")
        detail_label.grid(row=row + 1, column=1, sticky="w", padx=STYLE_FORM_LABEL_PADX, pady=(0, 3))
        ttk.Frame(parent, width=1).grid(row=row + 1, column=0, sticky="w", padx=STYLE_FORM_TOGGLE_PADX, pady=(0, 3))
        detail_frame = ttk.Frame(parent)
        detail_frame.grid(row=row + 1, column=2, sticky="ew", padx=STYLE_FORM_VALUE_CONTAINER_PADX, pady=(0, 3))
        if len(detail_defs) <= 2:
            _configure_two_slot_value_columns(detail_frame)
            for i, field in enumerate(detail_defs):
                caption = str(field.get("label", "")).strip()
                label_col = i * 2
                input_col = label_col + 1
                if caption:
                    ttk.Label(detail_frame, text=caption).grid(row=0, column=label_col, sticky="w", padx=(0, 4))
                else:
                    ttk.Frame(detail_frame, width=1).grid(row=0, column=label_col, sticky="w")
                controls.append(
                    create_compact_input_control(
                        app,
                        container=detail_frame,
                        field=field,
                        row_label=row_label,
                        label_widget=detail_label,
                        group_toggle_path=toggle_path,
                        grid_row=0,
                        widget_col=input_col,
                        is_last=(i == len(detail_defs) - 1),
                        is_single_full_width=False,
                        label_prefix=f"{row_label}:detail:{field.get('label', '')}",
                    )
                )
        else:
            for i, field in enumerate(detail_defs):
                caption = str(field.get("label", "")).strip()
                base_col = i * 2
                if caption:
                    ttk.Label(detail_frame, text=caption).grid(row=0, column=base_col, sticky="w", padx=(0, 2))
                    widget_col = base_col + 1
                else:
                    widget_col = base_col
                controls.append(
                    create_compact_input_control(
                        app,
                        container=detail_frame,
                        field=field,
                        row_label=row_label,
                        label_widget=detail_label,
                        group_toggle_path=toggle_path,
                        grid_row=0,
                        widget_col=widget_col,
                        is_last=(i == len(detail_defs) - 1),
                        is_single_full_width=False,
                        label_prefix=f"{row_label}:detail:{field.get('label', '')}",
                    )
                )

    return controls, rows_used


def create_palette_style_row(
    app,
    parent: tk.Misc,
    *,
    row: int,
    row_label: str,
    graph_style: dict[str, Any],
    palette_fields: list[dict[str, Any]],
    toggle: dict[str, Any] | None = None,
    summary_max_chars: int = 56,
    summary_label_width: int = 28,
) -> tuple[list[dict[str, Any]], int]:
    """col2 にサマリ + 設定ボタンを置く行を構築する。"""

    controls: list[dict[str, Any]] = []
    rows_used = 1
    label_widget = ttk.Label(parent, text=row_label)
    label_widget.grid(row=row, column=1, sticky="w", padx=STYLE_FORM_LABEL_PADX, pady=3)

    toggle_path: str | None = None
    if isinstance(toggle, dict) and str(toggle.get("path", "")).strip():
        toggle_path = str(toggle.get("path", "")).strip()
        var: tk.Variable = tk.BooleanVar(value=False)
        toggle_widget = ttk.Checkbutton(parent, text="", width=1, variable=var, command=app._on_style_form_commit)
        toggle_widget.grid(row=row, column=0, sticky="w", padx=STYLE_FORM_TOGGLE_PADX, pady=3)
        controls.append(
            {
                "path": toggle_path,
                "label": f"{row_label}:toggle",
                "kind": "bool",
                "var": var,
                "label_widget": label_widget,
                "widget": toggle_widget,
                "tooltip": str(toggle.get("tooltip", "")).strip(),
                "is_group_toggle": True,
            }
        )
    else:
        ttk.Frame(parent, width=1).grid(row=row, column=0, sticky="w", padx=STYLE_FORM_TOGGLE_PADX, pady=3)

    area = ttk.Frame(parent)
    area.grid(row=row, column=2, sticky="ew", padx=STYLE_FORM_VALUE_CONTAINER_PADX, pady=3)
    area.columnconfigure(0, weight=1)
    effective_max_chars = min(summary_max_chars, max(4, summary_label_width - 1))
    summary_var = tk.StringVar(value=build_palette_summary(graph_style, palette_fields, max_chars=effective_max_chars))
    summary_label = ttk.Label(area, textvariable=summary_var, foreground="#334155", width=summary_label_width, anchor="w")
    summary_label.grid(row=0, column=0, sticky="w")
    button = ttk.Button(
        area,
        text="設定...",
        command=lambda: app._open_palette_dialog(
            title=row_label,
            fields=palette_fields,
            group_toggle_path=toggle_path,
        ),
    )
    button.grid(row=0, column=1, sticky="e", padx=(8, 0))
    controls.append(
        {
            "path": f"__palette__:{row_label}",
            "label": row_label,
            "kind": "button",
            "var": tk.StringVar(value=""),
            "label_widget": label_widget,
            "widget": button,
            "group_toggle_path": toggle_path,
            "widget_state_on": "normal",
        }
    )
    app._style_palette_rows.append(
        {
            "row_label": row_label,
            "summary_var": summary_var,
            "fields": palette_fields,
            "summary_max_chars": summary_max_chars,
            "summary_label_width": summary_label_width,
            "group_toggle_path": toggle_path,
            "button": button,
        }
    )
    return controls, rows_used


def _configure_two_slot_value_columns(container: ttk.Frame) -> None:
    """2入力行を同一フォーマットで揃えるための固定列構成。"""

    container.columnconfigure(0, minsize=_VALUE_LABEL_COL_MINSIZE, weight=0)
    container.columnconfigure(1, minsize=_VALUE_INPUT_COL_MINSIZE, weight=0, uniform="value_inputs")
    container.columnconfigure(2, minsize=_VALUE_LABEL_COL_MINSIZE, weight=0)
    container.columnconfigure(3, minsize=_VALUE_INPUT_COL_MINSIZE, weight=0, uniform="value_inputs")


def build_palette_summary(graph_style: dict[str, Any], fields: list[dict[str, Any]], *, max_chars: int = 56) -> str:
    parts: list[str] = []
    for field in fields:
        path = str(field.get("path", "")).strip()
        if not path:
            continue
        label = str(field.get("label", "")).strip() or path.split(".")[-1]
        value = nested_value(graph_style, path, None)
        if value is None or value == "":
            continue
        parts.append(f"{label}:{value}")
    summary = " / ".join(parts) if parts else "未設定"
    if len(summary) > max_chars:
        return summary[:max_chars] + "..."
    return summary


def style_label_column_minsize(fields: list[dict[str, Any]]) -> int:
    """スタイルフォームのラベル列最小幅を返す。"""

    try:
        font = tkfont.nametofont("TkDefaultFont")
    except Exception:  # noqa: BLE001
        return 120
    max_px = 0
    for field in fields:
        label = str(field.get("label", "")).strip()
        max_px = max(max_px, int(font.measure(label)))
    return max(120, max_px + 18)
