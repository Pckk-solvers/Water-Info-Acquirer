from __future__ import annotations

import tkinter as tk
from tkinter import colorchooser, messagebox, ttk

from .style_payload import delete_nested_value, nested_value, set_nested_value


def is_hex_color(value: str) -> bool:
    text = str(value).strip()
    if len(text) not in (7, 9):
        return False
    if not text.startswith("#"):
        return False
    try:
        int(text[1:], 16)
        return True
    except ValueError:
        return False


def open_palette_dialog(app, *, title: str, fields: list[dict], group_toggle_path: str | None = None) -> None:
    graph_key = app._current_style_graph_key()
    graph_styles = app._style_payload.setdefault("graph_styles", {})
    graph_style = graph_styles.get(graph_key)
    if not isinstance(graph_style, dict):
        return

    if group_toggle_path:
        enabled = bool(nested_value(graph_style, group_toggle_path, False))
        if not enabled:
            app.preview_message.set(f"{title} は無効です（チェックをONにしてください）。")
            return

    dialog = tk.Toplevel(app)
    dialog.title(f"{title} の設定")
    dialog.transient(app)
    dialog.grab_set()
    dialog.resizable(False, False)
    dialog.columnconfigure(1, weight=1)

    controls: list[dict] = []
    for row, field in enumerate(fields):
        label = str(field.get("label", "")).strip()
        kind = str(field.get("kind", "str"))
        path = str(field.get("path", "")).strip()
        ttk.Label(dialog, text=label).grid(row=row, column=0, sticky="w", padx=(10, 6), pady=6)
        if kind == "choice":
            var = tk.StringVar(value=str(nested_value(graph_style, path, "") or ""))
            widget = ttk.Combobox(dialog, textvariable=var, state="readonly", values=tuple(field.get("values") or ()), width=20)
            widget.grid(row=row, column=1, sticky="ew", padx=(0, 10), pady=6)
        elif kind == "bool":
            var = tk.BooleanVar(value=bool(nested_value(graph_style, path, False)))
            widget = ttk.Checkbutton(dialog, text="", variable=var)
            widget.grid(row=row, column=1, sticky="w", padx=(0, 10), pady=6)
        elif kind == "color":
            raw = str(nested_value(graph_style, path, "") or "").strip()
            initial = raw if is_hex_color(raw) else "#000000"
            var = tk.StringVar(value=initial)
            row_frame = ttk.Frame(dialog)
            row_frame.grid(row=row, column=1, sticky="ew", padx=(0, 10), pady=6)
            row_frame.columnconfigure(0, weight=1)
            entry = ttk.Entry(row_frame, textvariable=var, width=16, state="readonly")
            entry.grid(row=0, column=0, sticky="w", padx=(0, 6))
            chip = tk.Label(row_frame, width=3, relief="solid", bg=initial)
            chip.grid(row=0, column=1, sticky="w", padx=(0, 6))

            def _pick_color(target_var=var, target_chip=chip) -> None:
                current = str(target_var.get()).strip()
                chosen = colorchooser.askcolor(color=current if is_hex_color(current) else "#000000", parent=dialog)
                if not chosen or not chosen[1]:
                    return
                hex_color = str(chosen[1]).upper()
                target_var.set(hex_color)
                target_chip.configure(bg=hex_color)

            choose_btn = ttk.Button(row_frame, text="選択...", command=_pick_color)
            choose_btn.grid(row=0, column=2, sticky="w")
            widget = choose_btn
        else:
            raw = nested_value(graph_style, path, "")
            var = tk.StringVar(value="" if raw is None else str(raw))
            widget = ttk.Entry(dialog, textvariable=var, width=22)
            widget.grid(row=row, column=1, sticky="ew", padx=(0, 10), pady=6)
        controls.append({"path": path, "kind": kind, "label": label or path, "var": var})

    buttons = ttk.Frame(dialog)
    buttons.grid(row=len(fields), column=0, columnspan=2, sticky="e", padx=10, pady=(4, 10))
    ttk.Button(buttons, text="キャンセル", command=dialog.destroy).grid(row=0, column=0, padx=(0, 6))

    def _apply() -> None:
        current_graph_styles = app._style_payload.setdefault("graph_styles", {})
        current_graph_style = current_graph_styles.get(graph_key)
        if not isinstance(current_graph_style, dict):
            current_graph_style = {}
            current_graph_styles[graph_key] = current_graph_style
        for control in controls:
            path = str(control["path"])
            kind = str(control["kind"])
            label = str(control["label"])
            var = control["var"]
            if kind == "bool":
                value = bool(var.get())
            elif kind == "choice":
                value = str(var.get()).strip()
            elif kind == "float":
                text = str(var.get()).strip()
                if text == "":
                    delete_nested_value(current_graph_style, path)
                    continue
                try:
                    value = float(text)
                except ValueError:
                    messagebox.showerror("入力エラー", f"{label} は数値で入力してください。", parent=dialog)
                    return
            elif kind == "int":
                text = str(var.get()).strip()
                if text == "":
                    delete_nested_value(current_graph_style, path)
                    continue
                try:
                    parsed = float(text)
                except ValueError:
                    messagebox.showerror("入力エラー", f"{label} は整数で入力してください。", parent=dialog)
                    return
                if not parsed.is_integer():
                    messagebox.showerror("入力エラー", f"{label} は整数で入力してください。", parent=dialog)
                    return
                value = int(parsed)
            elif kind == "color":
                text = str(var.get()).strip().upper()
                if text == "":
                    delete_nested_value(current_graph_style, path)
                    continue
                if not is_hex_color(text):
                    messagebox.showerror("入力エラー", f"{label} は #RRGGBB 形式の色を選択してください。", parent=dialog)
                    return
                value = text
            else:
                value = str(var.get()).strip()
            set_nested_value(current_graph_style, path, value)

        app._apply_group_toggle_states()
        app._set_style_text_from_payload()
        app._push_style_history(app._style_payload)
        app._refresh_style_forms_from_payload()
        app._render_preview(silent_json_error=True)

    ttk.Button(buttons, text="適用", command=_apply).grid(row=0, column=1)
