from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from .style_form_builder import STYLE_FORM_TOGGLE_COLUMN_MINSIZE, STYLE_FORM_LABEL_PADX, STYLE_FORM_VALUE_CONTAINER_PADX
from .tooltip import ToolTip


def build_style_tab(app, parent: ttk.Frame) -> None:
    """スタイル調整タブのレイアウトを構築する。"""

    parent.columnconfigure(0, weight=0)
    parent.columnconfigure(1, weight=1)
    parent.columnconfigure(0, minsize=560)
    parent.rowconfigure(0, weight=1)

    left = ttk.Frame(parent, padding=6)
    left.grid(row=0, column=0, sticky="nsew")
    left.columnconfigure(0, weight=1)
    left.rowconfigure(2, weight=1)
    ttk.Label(left, text="スタイル設定", font=("", 11, "bold")).grid(row=0, column=0, sticky="w")

    style_btns = ttk.Frame(left)
    style_btns.grid(row=1, column=0, sticky="ew", pady=(4, 6))
    apply_btn = ttk.Button(style_btns, text="反映", command=app._on_style_form_commit)
    apply_btn.grid(row=0, column=0, padx=(0, 4))
    undo_btn = ttk.Button(style_btns, text="↶", width=3, command=app._undo_style_change)
    undo_btn.grid(row=0, column=1, padx=(0, 4))
    redo_btn = ttk.Button(style_btns, text="↷", width=3, command=app._redo_style_change)
    redo_btn.grid(row=0, column=2, padx=(0, 8))
    ttk.Separator(style_btns, orient="vertical").grid(row=0, column=3, sticky="ns", padx=(0, 8))
    ttk.Button(style_btns, text="読込", command=app._load_style_from_file).grid(row=0, column=4, padx=(0, 4))
    ttk.Button(style_btns, text="保存", command=app._save_style_to_file).grid(row=0, column=5, padx=(0, 4))
    ttk.Button(style_btns, text="初期化", command=app._reset_style).grid(row=0, column=6)
    app._style_tooltips = [
        ToolTip(
            apply_btn,
            "現在の入力をスタイルに反映します。\n各項目で Enter を押しても同じ処理を実行できます。\n反映後にプレビューを更新します。",
        ),
        ToolTip(undo_btn, "元に戻す\nショートカット: Ctrl+Z"),
        ToolTip(redo_btn, "進む（取り消しを戻す）\nショートカット: Ctrl+Y / Ctrl+Shift+Z"),
    ]

    scroll_host = ttk.Frame(left)
    scroll_host.grid(row=2, column=0, sticky="nsew")
    scroll_host.columnconfigure(0, weight=1)
    scroll_host.rowconfigure(0, weight=1)

    app.style_scroll_canvas = tk.Canvas(scroll_host, highlightthickness=0, borderwidth=0)
    app.style_scroll_canvas.grid(row=0, column=0, sticky="nsew")
    style_scrollbar = ttk.Scrollbar(scroll_host, orient="vertical", command=app.style_scroll_canvas.yview)
    style_scrollbar.grid(row=0, column=1, sticky="ns")
    app.style_scroll_canvas.configure(yscrollcommand=style_scrollbar.set)

    scroll_inner = ttk.Frame(app.style_scroll_canvas)
    app._style_scroll_inner = scroll_inner
    app._style_scroll_window = app.style_scroll_canvas.create_window((0, 0), window=scroll_inner, anchor="nw")
    scroll_inner.columnconfigure(0, weight=1)
    scroll_inner.rowconfigure(2, weight=1)

    def _on_inner_configure(_event=None) -> None:
        app.style_scroll_canvas.configure(scrollregion=app.style_scroll_canvas.bbox("all"))

    def _on_canvas_configure(event=None) -> None:
        if event is None:
            return
        app.style_scroll_canvas.itemconfigure(app._style_scroll_window, width=event.width)

    def _on_mousewheel(event=None):
        if event is None:
            return
        delta = getattr(event, "delta", 0)
        if delta != 0:
            app.style_scroll_canvas.yview_scroll(int(-1 * (delta / 120)), "units")
            return "break"
        num = getattr(event, "num", None)
        if num == 4:
            app.style_scroll_canvas.yview_scroll(-1, "units")
            return "break"
        if num == 5:
            app.style_scroll_canvas.yview_scroll(1, "units")
            return "break"
        return None

    def _bind_mousewheel(_event=None) -> None:
        app.style_scroll_canvas.bind_all("<MouseWheel>", _on_mousewheel)
        app.style_scroll_canvas.bind_all("<Button-4>", _on_mousewheel)
        app.style_scroll_canvas.bind_all("<Button-5>", _on_mousewheel)

    def _unbind_mousewheel(_event=None) -> None:
        app.style_scroll_canvas.unbind_all("<MouseWheel>")
        app.style_scroll_canvas.unbind_all("<Button-4>")
        app.style_scroll_canvas.unbind_all("<Button-5>")

    scroll_inner.bind("<Configure>", _on_inner_configure)
    app.style_scroll_canvas.bind("<Configure>", _on_canvas_configure)
    app.style_scroll_canvas.bind("<Enter>", _bind_mousewheel)
    app.style_scroll_canvas.bind("<Leave>", _unbind_mousewheel)

    display_mode_box = ttk.LabelFrame(scroll_inner, text="共通設定")
    display_mode_box.grid(row=0, column=0, sticky="ew", pady=(0, 6))
    display_mode_box.columnconfigure(0, minsize=STYLE_FORM_TOGGLE_COLUMN_MINSIZE)
    display_mode_box.columnconfigure(1, weight=0)
    display_mode_box.columnconfigure(2, weight=1)
    app.display_mode_box = display_mode_box
    ttk.Label(display_mode_box, text="時刻表記").grid(row=0, column=1, padx=STYLE_FORM_LABEL_PADX, pady=6, sticky="w")
    mode_buttons = ttk.Frame(display_mode_box)
    mode_buttons.grid(row=0, column=2, padx=STYLE_FORM_VALUE_CONTAINER_PADX, pady=6, sticky="w")
    ttk.Radiobutton(
        mode_buttons,
        text="1時~24時",
        variable=app.time_display_mode,
        value="24h",
    ).grid(row=0, column=0, padx=(0, 3), sticky="w")
    ttk.Radiobutton(
        mode_buttons,
        text="datetime",
        variable=app.time_display_mode,
        value="datetime",
    ).grid(row=0, column=1, sticky="w")

    app.graph_style_box = ttk.LabelFrame(scroll_inner, text="グラフ別設定")
    app.graph_style_box.grid(row=1, column=0, sticky="ew", pady=(0, 6))
    app.graph_style_box.columnconfigure(1, weight=1)

    json_box = ttk.LabelFrame(scroll_inner, text="高度設定(JSON)")
    json_box.grid(row=2, column=0, sticky="nsew")
    json_box.columnconfigure(0, weight=1)
    json_box.rowconfigure(0, weight=1)
    app.style_text = tk.Text(json_box, wrap="none", undo=True, autoseparators=True, maxundo=2000)
    app.style_text.grid(row=0, column=0, sticky="nsew", padx=6, pady=6)
    app._set_style_text_from_payload()
    app.style_text.bind("<KeyRelease>", app._on_style_text_changed)
    app.bind("<Control-z>", app._on_undo_shortcut)
    app.bind("<Control-y>", app._on_redo_shortcut)
    app.bind("<Control-Z>", app._on_redo_shortcut)

    right = ttk.Frame(parent, padding=6)
    right.grid(row=0, column=1, sticky="nsew")
    right.columnconfigure(0, weight=1)
    right.rowconfigure(1, weight=1)

    preview_target = ttk.LabelFrame(right, text="プレビュー出力対象")
    preview_target.grid(row=0, column=0, sticky="ew", pady=(0, 6))
    preview_target.columnconfigure(1, weight=2)
    preview_target.columnconfigure(3, weight=1)
    preview_target.columnconfigure(5, weight=2)
    app.preview_station_combo = ttk.Combobox(
        preview_target,
        textvariable=app.preview_target_station,
        state="readonly",
        width=36,
    )
    app.preview_station_combo.bind("<<ComboboxSelected>>", app._on_preview_target_selection_changed)
    ttk.Label(preview_target, text="観測所").grid(row=0, column=0, padx=(6, 2), pady=6, sticky="w")
    app.preview_station_combo.grid(row=0, column=1, padx=(0, 6), pady=6, sticky="ew")
    ttk.Label(preview_target, text="基準日").grid(row=0, column=2, padx=6, pady=6, sticky="w")
    app.preview_date_combo = ttk.Combobox(
        preview_target,
        textvariable=app.preview_target_date,
        state="readonly",
        width=12,
    )
    app.preview_date_combo.bind("<<ComboboxSelected>>", app._on_preview_target_selection_changed)
    app.preview_date_combo.grid(row=0, column=3, padx=(0, 6), pady=6, sticky="ew")
    ttk.Label(preview_target, text="対象グラフ").grid(row=0, column=4, padx=6, pady=6, sticky="w")
    app.preview_graph_combo = ttk.Combobox(
        preview_target,
        textvariable=app.preview_target_graph,
        state="readonly",
        width=28,
    )
    app.preview_graph_combo.grid(row=0, column=5, padx=(0, 6), pady=6, sticky="ew")
    app.preview_graph_combo.bind("<<ComboboxSelected>>", app._on_preview_graph_selected)
    button_col = 6
    if getattr(app, "developer_mode", False):
        sample_btn = ttk.Button(preview_target, text="サンプル出力", command=app._export_preview_sample)
        sample_btn.grid(row=0, column=button_col, padx=(0, 6), pady=6)
        app._style_tooltips.append(
            ToolTip(
                sample_btn,
                "現在プレビュー中の1枚をPNG出力します。\n保存先は outputs/hydrology_graphs/dev_preview_samples 配下で自動採番されます。",
            )
        )
        button_col += 1
    ttk.Button(preview_target, text="プレビュー更新", command=app._render_preview).grid(
        row=0,
        column=button_col,
        padx=(0, 6),
        pady=6,
        sticky="e",
    )

    preview_box = ttk.LabelFrame(right, text="プレビュー")
    preview_box.grid(row=1, column=0, sticky="nsew")
    preview_box.columnconfigure(0, weight=1)
    preview_box.rowconfigure(0, weight=1)
    app.preview_viewport = tk.Frame(preview_box, bg="#4B5563")
    app.preview_viewport.grid(row=0, column=0, sticky="nsew", padx=6, pady=6)
    app.preview_viewport.columnconfigure(0, weight=1)
    app.preview_viewport.rowconfigure(0, weight=1)
    app.preview_viewport.grid_propagate(False)
    app.preview_viewport.configure(width=640, height=360)
    app.preview_canvas = tk.Canvas(
        app.preview_viewport,
        highlightthickness=0,
        borderwidth=0,
        background="#6B7280",
    )
    app.preview_canvas.grid(row=0, column=0, sticky="nsew")
    app.preview_canvas.bind("<Configure>", app._on_preview_area_resized)
    ttk.Label(preview_box, textvariable=app.preview_message, foreground="#475569").grid(
        row=1, column=0, sticky="w", padx=6, pady=(0, 6)
    )
    app._refresh_style_forms_from_payload()
