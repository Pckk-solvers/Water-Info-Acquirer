from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from .tooltip import ToolTip


def build_execute_tab(app, parent: ttk.Frame) -> None:
    """条件設定・実行タブのレイアウトを構築する。"""

    parent.rowconfigure(0, weight=1)
    parent.columnconfigure(0, weight=1)
    parent.columnconfigure(1, weight=1)

    left = ttk.Frame(parent)
    left.grid(row=0, column=0, sticky="nsew", padx=(0, 8))
    left.columnconfigure(0, weight=1)
    left.rowconfigure(2, weight=1)

    right = ttk.Frame(parent)
    right.grid(row=0, column=1, sticky="nsew")
    right.columnconfigure(0, weight=1)
    right.rowconfigure(0, weight=1)
    right.rowconfigure(1, weight=1)

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

    station_base_box = ttk.Frame(left)
    station_base_box.grid(row=2, column=0, sticky="nsew", pady=(0, 6))
    station_base_box.columnconfigure(0, weight=1)
    station_base_box.columnconfigure(1, weight=0)
    station_base_box.rowconfigure(0, weight=1)

    station_box = ttk.LabelFrame(station_base_box, text="観測所選択（チェック式）")
    station_box.grid(row=0, column=0, sticky="nsew", padx=(0, 6))
    station_box.columnconfigure(0, weight=1)
    station_box.rowconfigure(0, weight=1)
    app.station_list = tk.Listbox(station_box, selectmode="browse", activestyle="none", exportselection=False)
    app.station_list.grid(row=0, column=0, sticky="nsew", padx=6, pady=6)
    station_scroll = ttk.Scrollbar(station_box, command=app.station_list.yview)
    station_scroll.grid(row=0, column=1, sticky="ns", pady=6)
    app.station_list.configure(yscrollcommand=station_scroll.set)
    app.station_list.bind("<Button-1>", app._on_station_list_click)

    station_actions = ttk.Frame(station_box)
    station_actions.grid(row=1, column=0, columnspan=2, sticky="ew", padx=6, pady=(0, 6))
    station_actions.columnconfigure(0, weight=1)
    station_actions.columnconfigure(1, weight=1)
    app.btn_station_select_all = ttk.Button(station_actions, text="全選択", command=app._select_all_stations)
    app.btn_station_select_all.grid(row=0, column=0, sticky="w", padx=(0, 4))
    app.btn_station_clear_all = ttk.Button(station_actions, text="全解除", command=app._clear_all_stations)
    app.btn_station_clear_all.grid(row=0, column=1, sticky="e")

    base_date_box = ttk.LabelFrame(station_base_box, text="基準日設定")
    base_date_box.grid(row=0, column=1, sticky="ns")
    base_date_box.columnconfigure(0, weight=1)
    base_date_box.rowconfigure(2, weight=1)

    base_date_add_remove = ttk.Frame(base_date_box)
    base_date_add_remove.grid(row=0, column=0, sticky="ew", padx=4, pady=(6, 4))
    base_date_add_remove.columnconfigure(0, weight=1)
    base_date_add_remove.columnconfigure(1, weight=0)
    base_date_add_remove.columnconfigure(2, weight=0)
    app.btn_apply_station_checks = ttk.Button(base_date_add_remove, text="チェック反映", command=app._apply_station_checks)
    app.btn_apply_station_checks.grid(row=0, column=0, sticky="w")
    app.btn_add_base_date = ttk.Button(base_date_add_remove, text="追加", command=app._add_base_date_from_candidate)
    app.btn_add_base_date.grid(row=0, column=1, sticky="e", padx=(0, 4))
    app.btn_remove_base_date = ttk.Button(base_date_add_remove, text="削除", command=app._remove_selected_base_dates)
    app.btn_remove_base_date.grid(row=0, column=2, sticky="e")

    base_date_input = ttk.Frame(base_date_box)
    base_date_input.grid(row=1, column=0, sticky="ew", padx=4, pady=(0, 4))
    base_date_input.columnconfigure(2, weight=1)
    app.base_date_year_combo = ttk.Combobox(
        base_date_input,
        textvariable=app.base_date_year,
        state="readonly",
        width=6,
    )
    app.base_date_year_combo.grid(row=0, column=0, padx=(0, 2))
    app.base_date_year_combo.bind("<<ComboboxSelected>>", app._on_base_date_year_changed)
    app.base_date_month_combo = ttk.Combobox(
        base_date_input,
        textvariable=app.base_date_month,
        state="readonly",
        width=6,
    )
    app.base_date_month_combo.grid(row=0, column=1, padx=(0, 2))
    app.base_date_month_combo.bind("<<ComboboxSelected>>", app._on_base_date_month_changed)
    app.base_date_candidate_combo = ttk.Combobox(
        base_date_input,
        textvariable=app.base_date_candidate,
        state="readonly",
        width=6,
    )
    app.base_date_candidate_combo.grid(row=0, column=2, sticky="ew")

    app.base_date_list = tk.Listbox(base_date_box, selectmode="extended")
    app.base_date_list.grid(row=2, column=0, sticky="nsew", padx=4, pady=(0, 4))

    base_date_actions = ttk.Frame(base_date_box)
    base_date_actions.grid(row=3, column=0, sticky="ew", padx=4, pady=(0, 6))
    base_date_actions.columnconfigure(0, weight=1)
    base_date_actions.columnconfigure(1, weight=1)
    base_date_actions.columnconfigure(2, weight=1)
    app.btn_clear_base_date = ttk.Button(base_date_actions, text="全削除", command=app._clear_base_dates)
    app.btn_clear_base_date.grid(row=0, column=0, sticky="ew", padx=(0, 2))
    app.btn_import_base_date_csv = ttk.Button(base_date_actions, text="CSV読込", command=app._import_base_dates_csv)
    app.btn_import_base_date_csv.grid(row=0, column=1, sticky="ew", padx=(0, 2))
    app.btn_export_base_date_csv = ttk.Button(base_date_actions, text="CSV保存", command=app._export_base_dates_csv)
    app.btn_export_base_date_csv.grid(row=0, column=2, sticky="ew")

    graph_box = ttk.LabelFrame(left, text="グラフ種別")
    graph_box.grid(row=3, column=0, sticky="ew", pady=(0, 6))
    for col in range(4):
        graph_box.columnconfigure(col, weight=1 if col else 0)
    app.graph_type_vars = {}
    app.graph_cell_vars = {}

    col_item = ttk.Label(graph_box, text="項目")
    col_item.grid(row=0, column=0, sticky="w", padx=6, pady=(4, 2))
    col_3day = ttk.Label(graph_box, text="±1日（3日窓）")
    col_3day.grid(row=0, column=1, sticky="w", padx=6, pady=(4, 2))
    col_5day = ttk.Label(graph_box, text="±2日（5日窓）")
    col_5day.grid(row=0, column=2, sticky="w", padx=6, pady=(4, 2))
    col_annual = ttk.Label(graph_box, text="年最大")
    col_annual.grid(row=0, column=3, sticky="w", padx=6, pady=(4, 2))

    app._execute_tooltips = [
        ToolTip(app.parquet_entry, "入力Parquetディレクトリです。スキャンで観測所一覧を更新します。"),
        ToolTip(app.btn_browse_parquet, "Parquetディレクトリを選択します。"),
        ToolTip(app.btn_scan, "軽量スキャンを実行し、観測所一覧を更新します。"),
        ToolTip(app.threshold_entry, "基準線定義ファイル（CSV/JSON）のパスです。未指定でも実行できます。"),
        ToolTip(app.btn_browse_threshold, "基準線定義ファイルを選択します。"),
        ToolTip(app.station_list, "先頭の☐/☑をクリックして観測所を選択します。"),
        ToolTip(app.btn_station_select_all, "観測所をすべて選択状態にします。"),
        ToolTip(app.btn_station_clear_all, "観測所の選択をすべて解除します。"),
        ToolTip(app.btn_apply_station_checks, "現在の観測所チェックを基準日候補へ反映します。"),
        ToolTip(app.base_date_year_combo, "基準日の年を選択します。"),
        ToolTip(app.base_date_month_combo, "基準日の月を選択します。"),
        ToolTip(app.base_date_candidate_combo, "基準日の日を選択します。"),
        ToolTip(app.btn_add_base_date, "選択中の候補日を基準日リストへ追加します。"),
        ToolTip(app.btn_remove_base_date, "基準日リストの選択項目を削除します。"),
        ToolTip(app.base_date_list, "実行前検証・バッチ実行で使う基準日一覧です。"),
        ToolTip(app.btn_clear_base_date, "基準日リストをすべて削除します。"),
        ToolTip(app.btn_import_base_date_csv, "base_date列を持つCSVから基準日を読み込みます。"),
        ToolTip(app.btn_export_base_date_csv, "基準日リストをCSV（base_date列）で保存します。"),
        ToolTip(col_item, "雨量・流量・水位の系統を表します。"),
        ToolTip(col_3day, "基準日の前後1日を含む3日間（基準日-1日〜+1日）を対象にします。"),
        ToolTip(col_5day, "基準日の前後2日を含む5日間（基準日-2日〜+2日）を対象にします。"),
        ToolTip(col_annual, "基準日を使わず、各年の最大値系列を対象にします。"),
    ]

    rows = (
        ("雨量", "hyetograph", "annual_max_rainfall", "ハイエト（3日）", "ハイエト（5日）"),
        ("流量", "hydrograph_discharge", "annual_max_discharge", "流量ハイドロ（3日）", "流量ハイドロ（5日）"),
        ("水位", "hydrograph_water_level", "annual_max_water_level", "水位ハイドロ（3日）", "水位ハイドロ（5日）"),
    )
    for row_index, (label, event_graph_type, annual_graph_type, event_3_label, event_5_label) in enumerate(rows, start=1):
        ttk.Label(graph_box, text=label).grid(row=row_index, column=0, sticky="w", padx=6, pady=3)

        var_3 = tk.BooleanVar(value=True)
        app.graph_cell_vars[f"{event_graph_type}:3day"] = var_3
        chk_3 = ttk.Checkbutton(graph_box, text=event_3_label, variable=var_3)
        chk_3.grid(row=row_index, column=1, sticky="w", padx=6, pady=3)
        app._graph_type_checkbuttons.append(chk_3)

        var_5 = tk.BooleanVar(value=False)
        app.graph_cell_vars[f"{event_graph_type}:5day"] = var_5
        chk_5 = ttk.Checkbutton(graph_box, text=event_5_label, variable=var_5)
        chk_5.grid(row=row_index, column=2, sticky="w", padx=6, pady=3)
        app._graph_type_checkbuttons.append(chk_5)

        var_annual = tk.BooleanVar(value=True)
        app.graph_cell_vars[annual_graph_type] = var_annual
        app.graph_type_vars[annual_graph_type] = var_annual
        annual_label = app.GRAPH_TYPE_LABELS.get(annual_graph_type, annual_graph_type)
        chk_annual = ttk.Checkbutton(graph_box, text=annual_label, variable=var_annual)
        chk_annual.grid(row=row_index, column=3, sticky="w", padx=6, pady=3)
        app._graph_type_checkbuttons.append(chk_annual)

    execute_box = ttk.LabelFrame(left, text="実行")
    execute_box.grid(row=4, column=0, sticky="ew")
    app.btn_precheck = ttk.Button(execute_box, text="実行前検証", command=app._run_precheck)
    app.btn_precheck.grid(row=0, column=0, padx=12, pady=6, sticky="w")
    app.run_btn = ttk.Button(execute_box, text="バッチ実行", command=app._start_batch_run)
    app.run_btn.grid(row=0, column=1, padx=(6, 6), pady=6, sticky="w")
    app.stop_btn = ttk.Button(execute_box, text="停止", command=app._request_stop, state="disabled")
    app.stop_btn.grid(row=0, column=2, padx=(0, 6), pady=6, sticky="w")
    ttk.Label(execute_box, text="状態:").grid(row=0, column=3, padx=(12, 4), pady=6, sticky="w")
    ttk.Label(execute_box, textvariable=app.batch_status).grid(row=0, column=4, padx=(0, 6), pady=6, sticky="w")

    result_box = ttk.LabelFrame(right, text="結果")
    result_box.grid(row=0, column=0, sticky="nsew", pady=(0, 6))
    result_box.columnconfigure(0, weight=1)
    result_box.rowconfigure(1, weight=1)
    app.precheck_summary = tk.StringVar(value="対象数: 0 / NG: 0")
    ttk.Label(result_box, textvariable=app.precheck_summary).grid(row=0, column=0, sticky="w", padx=6, pady=(6, 4))

    cols = ("target", "status", "reason")
    app.result_tree = ttk.Treeview(result_box, columns=cols, show="headings")
    for key, text, width in (
        ("target", "対象", 290),
        ("status", "状態", 100),
        ("reason", "理由", 180),
    ):
        app.result_tree.heading(key, text=text)
        app.result_tree.column(
            key,
            width=width,
            minwidth=48 if key == "status" else 120,
            stretch=True,
            anchor="w" if key in ("target", "reason") else "center",
        )
    app.result_tree.grid(row=1, column=0, sticky="nsew", padx=6, pady=(0, 6))
    app.result_tree.bind("<Double-1>", app._on_result_row_double_click)
    result_scroll = ttk.Scrollbar(result_box, command=app.result_tree.yview)
    result_scroll.grid(row=1, column=1, sticky="ns", pady=(0, 6))
    app.result_tree.configure(yscrollcommand=result_scroll.set)

    log_box = ttk.LabelFrame(right, text="ログ")
    log_box.grid(row=1, column=0, sticky="nsew")
    log_box.columnconfigure(0, weight=1)
    log_box.rowconfigure(0, weight=1)
    app.log_text = tk.Text(log_box, wrap="none")
    app.log_text.grid(row=0, column=0, sticky="nsew")
    log_scroll = ttk.Scrollbar(log_box, command=app.log_text.yview)
    log_scroll.grid(row=0, column=1, sticky="ns")
    app.log_text.configure(yscrollcommand=log_scroll.set)

    app._execute_tooltips.extend(
        [
            ToolTip(app.btn_precheck, "選択条件で実行可否を検証し、READY/NGを結果表に表示します。"),
            ToolTip(app.run_btn, "READY対象をまとめてバッチ実行します。"),
            ToolTip(app.stop_btn, "実行中のバッチ処理に停止要求を送ります。"),
            ToolTip(app.result_tree, "結果行をダブルクリックすると、その出力先フルパスを表示します。"),
            ToolTip(app.log_text, "スキャン・検証・実行のログを表示します。"),
        ]
    )

    app._execution_disable_widgets = [
        app.parquet_entry,
        app.threshold_entry,
        app.btn_browse_parquet,
        app.btn_scan,
        app.btn_browse_threshold,
        app.station_list,
        app.btn_station_select_all,
        app.btn_station_clear_all,
        app.base_date_year_combo,
        app.base_date_month_combo,
        app.base_date_candidate_combo,
        app.btn_apply_station_checks,
        app.btn_add_base_date,
        app.base_date_list,
        app.btn_remove_base_date,
        app.btn_clear_base_date,
        app.btn_import_base_date_csv,
        app.btn_export_base_date_csv,
        app.btn_precheck,
    ]
