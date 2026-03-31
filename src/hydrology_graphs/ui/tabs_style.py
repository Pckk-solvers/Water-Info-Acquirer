from __future__ import annotations

import tkinter as tk
from tkinter import ttk


def build_style_tab(app, parent: ttk.Frame) -> None:
    """スタイル調整タブのレイアウトを構築する。"""

    parent.columnconfigure(0, weight=2)
    parent.columnconfigure(1, weight=3)
    parent.rowconfigure(0, weight=1)

    left = ttk.Frame(parent, padding=6)
    left.grid(row=0, column=0, sticky="nsew")
    left.columnconfigure(0, weight=1)
    left.rowconfigure(4, weight=1)
    ttk.Label(left, text="スタイル設定", font=("", 11, "bold")).grid(row=0, column=0, sticky="w")

    common_box = ttk.LabelFrame(left, text="共通設定")
    common_box.grid(row=1, column=0, sticky="ew", pady=(6, 6))
    common_box.columnconfigure(1, weight=1)
    app._style_common_controls = []
    for row, field in enumerate(app.COMMON_STYLE_FIELDS):
        control = app._create_style_control(common_box, row=row, field=field)
        app._style_common_controls.append(control)
    ttk.Label(
        common_box,
        text="※ DPIは出力品質の設定です。プレビュー表示サイズは幅・高さを基準に調整されます。",
        foreground="#475569",
    ).grid(row=len(app.COMMON_STYLE_FIELDS), column=0, columnspan=2, sticky="w", padx=6, pady=(2, 4))

    app.graph_style_box = ttk.LabelFrame(left, text="グラフ別設定")
    app.graph_style_box.grid(row=2, column=0, sticky="ew", pady=(0, 6))
    app.graph_style_box.columnconfigure(1, weight=1)

    style_btns = ttk.Frame(left)
    style_btns.grid(row=3, column=0, sticky="ew", pady=(0, 4))
    ttk.Button(style_btns, text="フォーム適用", command=app._on_style_form_commit).grid(row=0, column=0, padx=(0, 4))
    ttk.Button(style_btns, text="元に戻す(Ctrl+Z)", command=app._undo_style_change).grid(row=0, column=1, padx=(0, 4))
    ttk.Button(style_btns, text="やり直し(Ctrl+Y)", command=app._redo_style_change).grid(row=0, column=2, padx=(0, 8))
    ttk.Separator(style_btns, orient="vertical").grid(row=0, column=3, sticky="ns", padx=(0, 8))
    ttk.Button(style_btns, text="読込", command=app._load_style_from_file).grid(row=0, column=4, padx=(0, 4))
    ttk.Button(style_btns, text="保存", command=app._save_style_to_file).grid(row=0, column=5, padx=(0, 4))
    ttk.Button(style_btns, text="初期化", command=app._reset_style).grid(row=0, column=6)

    json_box = ttk.LabelFrame(left, text="高度設定(JSON)")
    json_box.grid(row=4, column=0, sticky="nsew")
    json_box.columnconfigure(0, weight=1)
    json_box.rowconfigure(0, weight=1)
    app.style_text = tk.Text(json_box, wrap="none", undo=True, autoseparators=True, maxundo=2000)
    app.style_text.grid(row=0, column=0, sticky="nsew", padx=(6, 0), pady=6)
    json_scroll = ttk.Scrollbar(json_box, command=app.style_text.yview)
    json_scroll.grid(row=0, column=1, sticky="ns", padx=(0, 6), pady=6)
    app.style_text.configure(yscrollcommand=json_scroll.set)
    app._set_style_text_from_payload()
    app.style_text.bind("<KeyRelease>", app._on_style_text_changed)
    app.bind("<Control-z>", app._on_undo_shortcut)
    app.bind("<Control-y>", app._on_redo_shortcut)
    app.bind("<Control-Z>", app._on_redo_shortcut)

    right = ttk.Frame(parent, padding=6)
    right.grid(row=0, column=1, sticky="nsew")
    right.columnconfigure(0, weight=1)
    right.rowconfigure(1, weight=1)

    preview_target = ttk.LabelFrame(right, text="プレビュー対象")
    preview_target.grid(row=0, column=0, sticky="ew", pady=(0, 6))
    ttk.Label(preview_target, text="観測所").grid(row=0, column=0, padx=6, pady=6)
    app.preview_station_combo = ttk.Combobox(
        preview_target,
        textvariable=app.preview_target_station,
        state="readonly",
        width=36,
    )
    app.preview_station_combo.grid(row=0, column=1, padx=6, pady=6)
    ttk.Label(preview_target, text="基準日").grid(row=0, column=2, padx=6, pady=6)
    app.preview_date_combo = ttk.Combobox(
        preview_target,
        textvariable=app.preview_target_date,
        state="readonly",
        width=12,
    )
    app.preview_date_combo.grid(row=0, column=3, padx=6, pady=6)
    ttk.Label(preview_target, text="グラフ").grid(row=0, column=4, padx=6, pady=6)
    app.preview_graph_combo = ttk.Combobox(
        preview_target,
        textvariable=app.preview_target_graph,
        values=list(app.GRAPH_TYPES),
        state="readonly",
        width=24,
    )
    app.preview_graph_combo.grid(row=0, column=5, padx=6, pady=6)
    app.preview_graph_combo.bind("<<ComboboxSelected>>", app._on_preview_graph_selected)
    ttk.Button(preview_target, text="プレビュー更新", command=app._render_preview).grid(row=0, column=6, padx=6, pady=6)

    preview_box = ttk.LabelFrame(right, text="プレビュー")
    preview_box.grid(row=1, column=0, sticky="nsew")
    preview_box.columnconfigure(0, weight=1)
    preview_box.rowconfigure(0, weight=1)
    app.preview_label = ttk.Label(preview_box, text="プレビュー未生成", anchor="center")
    app.preview_label.grid(row=0, column=0, sticky="nsew", padx=6, pady=6)
    ttk.Label(preview_box, textvariable=app.preview_message, foreground="#475569").grid(
        row=1, column=0, sticky="w", padx=6, pady=(0, 6)
    )
    app._refresh_style_forms_from_payload()
