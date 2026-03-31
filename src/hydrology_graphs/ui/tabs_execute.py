from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from hydrology_graphs.domain.constants import GRAPH_TYPES


def build_execute_tab(app, parent: ttk.Frame) -> None:
    """条件設定・実行タブのレイアウトを構築する。"""

    parent.rowconfigure(0, weight=3)
    parent.rowconfigure(2, weight=2)
    parent.columnconfigure(0, weight=1)

    top = ttk.Frame(parent)
    top.grid(row=0, column=0, sticky="nsew")
    top.columnconfigure(0, weight=3)
    top.columnconfigure(1, weight=2)
    top.rowconfigure(0, weight=1)

    left = ttk.Frame(top)
    left.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
    left.columnconfigure(0, weight=1)
    left.rowconfigure(3, weight=1)
    left.rowconfigure(4, weight=1)

    parquet_box = ttk.LabelFrame(left, text="Parquet入力")
    parquet_box.grid(row=0, column=0, sticky="ew", pady=(0, 6))
    parquet_box.columnconfigure(0, weight=1)
    app.parquet_entry = ttk.Entry(parquet_box, textvariable=app.parquet_dir)
    app.parquet_entry.grid(row=0, column=0, sticky="ew", padx=(6, 4), pady=6)
    app.btn_browse_parquet = ttk.Button(parquet_box, text="参照", command=app._browse_parquet_dir)
    app.btn_browse_parquet.grid(row=0, column=1, padx=(0, 4), pady=6)
    app.btn_scan = ttk.Button(parquet_box, text="スキャン", command=app._scan_parquet)
    app.btn_scan.grid(row=0, column=2, padx=(0, 6), pady=6)

    threshold_box = ttk.LabelFrame(left, text="基準線定義")
    threshold_box.grid(row=1, column=0, sticky="ew", pady=(0, 6))
    threshold_box.columnconfigure(0, weight=1)
    app.threshold_entry = ttk.Entry(threshold_box, textvariable=app.threshold_path)
    app.threshold_entry.grid(row=0, column=0, sticky="ew", padx=(6, 4), pady=6)
    app.btn_browse_threshold = ttk.Button(threshold_box, text="参照", command=app._browse_threshold_file)
    app.btn_browse_threshold.grid(row=0, column=1, padx=(0, 6), pady=6)

    graph_box = ttk.LabelFrame(left, text="グラフ種別")
    graph_box.grid(row=2, column=0, sticky="ew", pady=(0, 6))
    for col in range(3):
        graph_box.columnconfigure(col, weight=1)
    app.graph_type_vars = {}
    for idx, graph_type in enumerate(GRAPH_TYPES):
        var = tk.BooleanVar(value=True)
        app.graph_type_vars[graph_type] = var
        chk = ttk.Checkbutton(graph_box, text=app.GRAPH_TYPE_LABELS.get(graph_type, graph_type), variable=var)
        chk.grid(row=idx // 3, column=idx % 3, sticky="w", padx=6, pady=4)
        app._graph_type_checkbuttons.append(chk)

    station_box = ttk.LabelFrame(left, text="観測所選択（複数）")
    station_box.grid(row=3, column=0, sticky="nsew", pady=(0, 6))
    station_box.columnconfigure(0, weight=1)
    station_box.rowconfigure(0, weight=1)
    app.station_list = tk.Listbox(station_box, selectmode="extended", height=10)
    app.station_list.grid(row=0, column=0, sticky="nsew", padx=6, pady=6)
    station_scroll = ttk.Scrollbar(station_box, command=app.station_list.yview)
    station_scroll.grid(row=0, column=1, sticky="ns", pady=6)
    app.station_list.configure(yscrollcommand=station_scroll.set)

    base_date_box = ttk.LabelFrame(left, text="基準日設定（YYYY-MM-DD, 改行区切り）")
    base_date_box.grid(row=4, column=0, sticky="nsew", pady=(0, 6))
    base_date_box.columnconfigure(0, weight=1)
    base_date_box.rowconfigure(0, weight=1)
    app.base_dates_text = tk.Text(base_date_box, height=5)
    app.base_dates_text.grid(row=0, column=0, sticky="nsew", padx=6, pady=6)

    window_box = ttk.LabelFrame(left, text="イベント窓設定")
    window_box.grid(row=5, column=0, sticky="ew")
    app.radio_window_3 = ttk.Radiobutton(window_box, text="3日", value=3, variable=app.event_window_days)
    app.radio_window_3.grid(row=0, column=0, padx=6, pady=6)
    app.radio_window_5 = ttk.Radiobutton(window_box, text="5日", value=5, variable=app.event_window_days)
    app.radio_window_5.grid(row=0, column=1, padx=6, pady=6)
    app.btn_precheck = ttk.Button(window_box, text="実行前検証", command=app._run_precheck)
    app.btn_precheck.grid(row=0, column=2, padx=12, pady=6)

    right = ttk.LabelFrame(top, text="実行前検証結果")
    right.grid(row=0, column=1, sticky="nsew")
    right.columnconfigure(0, weight=1)
    right.rowconfigure(1, weight=1)
    app.precheck_summary = tk.StringVar(value="対象数: 0 / NG: 0")
    ttk.Label(right, textvariable=app.precheck_summary).grid(row=0, column=0, sticky="w", padx=6, pady=(6, 4))
    pre_columns = ("target", "status", "reason")
    app.precheck_tree = ttk.Treeview(right, columns=pre_columns, show="headings", height=18)
    app.precheck_tree.heading("target", text="対象")
    app.precheck_tree.heading("status", text="判定")
    app.precheck_tree.heading("reason", text="理由")
    app.precheck_tree.column("target", width=330, anchor="w")
    app.precheck_tree.column("status", width=80, anchor="center")
    app.precheck_tree.column("reason", width=280, anchor="w")
    app.precheck_tree.grid(row=1, column=0, sticky="nsew", padx=6, pady=(0, 6))
    pre_scroll = ttk.Scrollbar(right, command=app.precheck_tree.yview)
    pre_scroll.grid(row=1, column=1, sticky="ns", pady=(0, 6))
    app.precheck_tree.configure(yscrollcommand=pre_scroll.set)

    execute_ctrl = ttk.LabelFrame(parent, text="実行")
    execute_ctrl.grid(row=1, column=0, sticky="ew", pady=(8, 8))
    execute_ctrl.columnconfigure(1, weight=1)
    app.run_btn = ttk.Button(execute_ctrl, text="バッチ実行", command=app._start_batch_run)
    app.run_btn.grid(row=0, column=0, padx=(6, 6), pady=6)
    app.stop_btn = ttk.Button(execute_ctrl, text="停止", command=app._request_stop, state="disabled")
    app.stop_btn.grid(row=0, column=1, padx=(0, 6), pady=6, sticky="w")
    ttk.Label(execute_ctrl, text="状態:").grid(row=0, column=2, padx=(12, 4), pady=6, sticky="w")
    ttk.Label(execute_ctrl, textvariable=app.batch_status).grid(row=0, column=3, padx=(0, 6), pady=6, sticky="w")

    bottom = ttk.Frame(parent)
    bottom.grid(row=2, column=0, sticky="nsew")
    bottom.columnconfigure(0, weight=2)
    bottom.columnconfigure(1, weight=3)
    bottom.rowconfigure(0, weight=1)

    result_box = ttk.LabelFrame(bottom, text="実行結果")
    result_box.grid(row=0, column=0, sticky="nsew", padx=(0, 6))
    result_box.columnconfigure(0, weight=1)
    result_box.rowconfigure(0, weight=1)
    cols = ("target", "status", "reason", "path")
    app.batch_tree = ttk.Treeview(result_box, columns=cols, show="headings")
    for key, text, width in (
        ("target", "対象", 220),
        ("status", "結果", 70),
        ("reason", "理由", 220),
        ("path", "出力先", 280),
    ):
        app.batch_tree.heading(key, text=text)
        app.batch_tree.column(key, width=width, anchor="w" if key in ("target", "reason", "path") else "center")
    app.batch_tree.grid(row=0, column=0, sticky="nsew")
    result_scroll = ttk.Scrollbar(result_box, command=app.batch_tree.yview)
    result_scroll.grid(row=0, column=1, sticky="ns")
    app.batch_tree.configure(yscrollcommand=result_scroll.set)

    log_box = ttk.LabelFrame(bottom, text="ログ")
    log_box.grid(row=0, column=1, sticky="nsew")
    log_box.columnconfigure(0, weight=1)
    log_box.rowconfigure(0, weight=1)
    app.log_text = tk.Text(log_box, wrap="none")
    app.log_text.grid(row=0, column=0, sticky="nsew")
    log_scroll = ttk.Scrollbar(log_box, command=app.log_text.yview)
    log_scroll.grid(row=0, column=1, sticky="ns")
    app.log_text.configure(yscrollcommand=log_scroll.set)

    app._execution_disable_widgets = [
        app.parquet_entry,
        app.threshold_entry,
        app.btn_browse_parquet,
        app.btn_scan,
        app.btn_browse_threshold,
        app.station_list,
        app.base_dates_text,
        app.radio_window_3,
        app.radio_window_5,
        app.btn_precheck,
    ]
