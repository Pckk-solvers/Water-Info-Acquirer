from __future__ import annotations

import base64
import json
import queue
import threading
import tkinter as tk
from copy import deepcopy
from datetime import date, datetime
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Any

import pandas as pd

from hydrology_graphs.domain.constants import GRAPH_TYPES
from hydrology_graphs.domain.models import GraphTarget
from hydrology_graphs.io.parquet_store import ParquetCatalog
from hydrology_graphs.io.style_store import default_style, save_style
from hydrology_graphs.io.threshold_store import ThresholdLoadResult, load_thresholds
from hydrology_graphs.services import BatchRunInput, BatchTarget, HydrologyGraphService, PrecheckInput, PreviewInput
from water_info_acquirer.app_meta import get_module_title
from water_info_acquirer.navigation import build_navigation_menu

"""水文グラフ生成 GUI の本体。

この画面では、Parquet のスキャン、実行前検証、スタイル調整、
プレビュー、バッチ実行をまとめて扱う。
"""


GRAPH_TYPE_LABELS = {
    "hyetograph": "ハイエトグラフ（雨量）",
    "hydrograph_discharge": "ハイドログラフ（流量）",
    "hydrograph_water_level": "ハイドログラフ（水位）",
    "annual_max_rainfall": "年最大雨量",
    "annual_max_discharge": "年最大流量",
    "annual_max_water_level": "年最高水位",
}

COMMON_STYLE_FIELDS: tuple[dict[str, Any], ...] = (
    {"path": "font_family", "label": "フォント", "kind": "str"},
    {"path": "font_size", "label": "基本フォントサイズ", "kind": "int"},
    {"path": "figure_width", "label": "図幅(inch)", "kind": "float"},
    {"path": "figure_height", "label": "図高(inch)", "kind": "float"},
    {"path": "dpi", "label": "DPI", "kind": "int"},
    {"path": "background_color", "label": "背景色(#RRGGBB)", "kind": "str"},
    {"path": "legend.enabled", "label": "凡例表示", "kind": "bool"},
    {"path": "grid.enabled", "label": "グリッド表示", "kind": "bool"},
)

BASE_GRAPH_STYLE_FIELDS: tuple[dict[str, Any], ...] = (
    {"path": "title.template", "label": "タイトルテンプレート", "kind": "str"},
    {"path": "axis.x_label", "label": "X軸ラベル", "kind": "str"},
    {"path": "axis.y_label", "label": "Y軸ラベル", "kind": "str"},
    {"path": "series_color", "label": "系列色", "kind": "str"},
    {"path": "series_width", "label": "系列幅", "kind": "float"},
    {
        "path": "series_style",
        "label": "系列線種",
        "kind": "choice",
        "values": ("solid", "dashed", "dashdot", "dotted"),
    },
    {"path": "x_axis.tick_rotation", "label": "X軸角度", "kind": "float"},
    {"path": "y_axis.tick_count", "label": "Y軸目盛数", "kind": "int"},
    {
        "path": "y_axis.number_format",
        "label": "Y軸数値形式",
        "kind": "choice",
        "values": ("plain", "comma", "percent"),
    },
)


class HydrologyGraphsApp(tk.Toplevel):
    """水文グラフ生成のメインウィンドウ。"""

    def __init__(
        self,
        *,
        parent: tk.Misc,
        on_open_other=None,
        on_close=None,
        on_return_home=None,
        developer_mode: bool = False,
    ) -> None:
        super().__init__(parent)
        self.on_open_other = on_open_other
        self.on_close = on_close
        self.on_return_home = on_return_home
        self.developer_mode = developer_mode
        self.service = HydrologyGraphService()

        base_title = get_module_title("hydrology_graphs", lang="jp")
        self.title(f"{base_title} [DEV]" if self.developer_mode else base_title)
        self.geometry("1520x900")
        self.minsize(1320, 760)
        self.configure(bg="#F8FAFC")
        self.protocol("WM_DELETE_WINDOW", self._on_close)

        self.parquet_dir = tk.StringVar(value="")
        self.threshold_path = tk.StringVar(value="")
        self.event_window_days = tk.IntVar(value=3)
        self.batch_status = tk.StringVar(value="待機中")
        self.preview_message = tk.StringVar(value="")
        self.preview_target_station = tk.StringVar(value="")
        self.preview_target_date = tk.StringVar(value="")
        self.preview_target_graph = tk.StringVar(value=GRAPH_TYPES[0])

        # 画面全体で共有する状態はここで初期化する。
        self._style_json_path: str | None = None
        self._style_payload: dict = default_style()
        self._style_debounce_id: str | None = None
        self._style_text_syncing = False
        self._style_form_updating = False
        self._style_common_controls: list[dict[str, Any]] = []
        self._style_graph_controls: list[dict[str, Any]] = []
        self._style_history: list[dict[str, Any]] = [deepcopy(self._style_payload)]
        self._style_history_index: int = 0
        self._style_history_applying = False
        self._preview_photo: tk.PhotoImage | None = None
        self._catalog: ParquetCatalog | None = None
        self._catalog_stations: list[tuple[str, str, str]] = []
        self._base_dates: list[str] = []
        self._precheck_ok_targets: list[GraphTarget] = []
        self._event_queue: queue.Queue[tuple[str, object]] = queue.Queue()
        self._running = False
        self._scan_running = False
        self._preview_running = False
        self._preview_pending: dict[str, Any] | None = None
        self._threshold_cache_path: str | None = None
        self._threshold_cache_mtime_ns: int | None = None
        self._threshold_cache_result: ThresholdLoadResult | None = None
        self._stop_event: threading.Event | None = None
        self._execution_disable_widgets: list[tk.Widget] = []
        self._graph_type_checkbuttons: list[ttk.Checkbutton] = []

        self.config(
            menu=build_navigation_menu(
                self,
                current_app_key="hydrology_graphs",
                on_open_other=self._open_other,
                on_return_home=self._return_home,
            )
        )
        self._build_ui()
        if self.developer_mode:
            self._activate_dev_dummy_mode()
        self.after(120, self._poll_events)

    def _build_ui(self) -> None:
        """画面全体の骨組みを作る。"""

        root = ttk.Frame(self, padding=12)
        root.pack(fill="both", expand=True)
        root.rowconfigure(1, weight=1)
        root.columnconfigure(0, weight=1)

        ttk.Label(root, text=get_module_title("hydrology_graphs", lang="jp"), font=("", 14, "bold")).grid(
            row=0, column=0, sticky="w"
        )

        self.notebook = ttk.Notebook(root)
        self.notebook.grid(row=1, column=0, sticky="nsew", pady=(8, 0))

        self.execute_tab = ttk.Frame(self.notebook)
        self.style_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.execute_tab, text="条件設定・実行")
        self.notebook.add(self.style_tab, text="スタイル調整")

        self._build_execute_tab(self.execute_tab)
        self._build_style_tab(self.style_tab)

    def _build_execute_tab(self, parent: ttk.Frame) -> None:
        """条件設定・実行タブを構築する。"""

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
        self.parquet_entry = ttk.Entry(parquet_box, textvariable=self.parquet_dir)
        self.parquet_entry.grid(row=0, column=0, sticky="ew", padx=(6, 4), pady=6)
        self.btn_browse_parquet = ttk.Button(parquet_box, text="参照", command=self._browse_parquet_dir)
        self.btn_browse_parquet.grid(row=0, column=1, padx=(0, 4), pady=6)
        self.btn_scan = ttk.Button(parquet_box, text="スキャン", command=self._scan_parquet)
        self.btn_scan.grid(row=0, column=2, padx=(0, 6), pady=6)

        threshold_box = ttk.LabelFrame(left, text="基準線定義")
        threshold_box.grid(row=1, column=0, sticky="ew", pady=(0, 6))
        threshold_box.columnconfigure(0, weight=1)
        self.threshold_entry = ttk.Entry(threshold_box, textvariable=self.threshold_path)
        self.threshold_entry.grid(row=0, column=0, sticky="ew", padx=(6, 4), pady=6)
        self.btn_browse_threshold = ttk.Button(threshold_box, text="参照", command=self._browse_threshold_file)
        self.btn_browse_threshold.grid(row=0, column=1, padx=(0, 6), pady=6)

        graph_box = ttk.LabelFrame(left, text="グラフ種別")
        graph_box.grid(row=2, column=0, sticky="ew", pady=(0, 6))
        # 3列×2行で見やすく並べる。
        for col in range(3):
            graph_box.columnconfigure(col, weight=1)
        self.graph_type_vars: dict[str, tk.BooleanVar] = {}
        for idx, graph_type in enumerate(GRAPH_TYPES):
            var = tk.BooleanVar(value=True)
            self.graph_type_vars[graph_type] = var
            chk = ttk.Checkbutton(graph_box, text=GRAPH_TYPE_LABELS.get(graph_type, graph_type), variable=var)
            chk.grid(row=idx // 3, column=idx % 3, sticky="w", padx=6, pady=4)
            self._graph_type_checkbuttons.append(chk)

        station_box = ttk.LabelFrame(left, text="観測所選択（複数）")
        station_box.grid(row=3, column=0, sticky="nsew", pady=(0, 6))
        station_box.columnconfigure(0, weight=1)
        station_box.rowconfigure(0, weight=1)
        self.station_list = tk.Listbox(station_box, selectmode="extended", height=10)
        self.station_list.grid(row=0, column=0, sticky="nsew", padx=6, pady=6)
        station_scroll = ttk.Scrollbar(station_box, command=self.station_list.yview)
        station_scroll.grid(row=0, column=1, sticky="ns", pady=6)
        self.station_list.configure(yscrollcommand=station_scroll.set)

        base_date_box = ttk.LabelFrame(left, text="基準日設定（YYYY-MM-DD, 改行区切り）")
        base_date_box.grid(row=4, column=0, sticky="nsew", pady=(0, 6))
        base_date_box.columnconfigure(0, weight=1)
        base_date_box.rowconfigure(0, weight=1)
        self.base_dates_text = tk.Text(base_date_box, height=5)
        self.base_dates_text.grid(row=0, column=0, sticky="nsew", padx=6, pady=6)

        window_box = ttk.LabelFrame(left, text="イベント窓設定")
        window_box.grid(row=5, column=0, sticky="ew")
        self.radio_window_3 = ttk.Radiobutton(window_box, text="3日", value=3, variable=self.event_window_days)
        self.radio_window_3.grid(row=0, column=0, padx=6, pady=6)
        self.radio_window_5 = ttk.Radiobutton(window_box, text="5日", value=5, variable=self.event_window_days)
        self.radio_window_5.grid(row=0, column=1, padx=6, pady=6)
        self.btn_precheck = ttk.Button(window_box, text="実行前検証", command=self._run_precheck)
        self.btn_precheck.grid(row=0, column=2, padx=12, pady=6)

        right = ttk.LabelFrame(top, text="実行前検証結果")
        right.grid(row=0, column=1, sticky="nsew")
        right.columnconfigure(0, weight=1)
        right.rowconfigure(1, weight=1)
        self.precheck_summary = tk.StringVar(value="対象数: 0 / NG: 0")
        ttk.Label(right, textvariable=self.precheck_summary).grid(row=0, column=0, sticky="w", padx=6, pady=(6, 4))
        pre_columns = ("target", "status", "reason")
        self.precheck_tree = ttk.Treeview(right, columns=pre_columns, show="headings", height=18)
        self.precheck_tree.heading("target", text="対象")
        self.precheck_tree.heading("status", text="判定")
        self.precheck_tree.heading("reason", text="理由")
        self.precheck_tree.column("target", width=330, anchor="w")
        self.precheck_tree.column("status", width=80, anchor="center")
        self.precheck_tree.column("reason", width=280, anchor="w")
        self.precheck_tree.grid(row=1, column=0, sticky="nsew", padx=6, pady=(0, 6))
        pre_scroll = ttk.Scrollbar(right, command=self.precheck_tree.yview)
        pre_scroll.grid(row=1, column=1, sticky="ns", pady=(0, 6))
        self.precheck_tree.configure(yscrollcommand=pre_scroll.set)

        execute_ctrl = ttk.LabelFrame(parent, text="実行")
        execute_ctrl.grid(row=1, column=0, sticky="ew", pady=(8, 8))
        execute_ctrl.columnconfigure(1, weight=1)
        self.run_btn = ttk.Button(execute_ctrl, text="バッチ実行", command=self._start_batch_run)
        self.run_btn.grid(row=0, column=0, padx=(6, 6), pady=6)
        self.stop_btn = ttk.Button(execute_ctrl, text="停止", command=self._request_stop, state="disabled")
        self.stop_btn.grid(row=0, column=1, padx=(0, 6), pady=6, sticky="w")
        ttk.Label(execute_ctrl, text="状態:").grid(row=0, column=2, padx=(12, 4), pady=6, sticky="w")
        ttk.Label(execute_ctrl, textvariable=self.batch_status).grid(row=0, column=3, padx=(0, 6), pady=6, sticky="w")

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
        self.batch_tree = ttk.Treeview(result_box, columns=cols, show="headings")
        for key, text, width in (
            ("target", "対象", 220),
            ("status", "結果", 70),
            ("reason", "理由", 220),
            ("path", "出力先", 280),
        ):
            self.batch_tree.heading(key, text=text)
            self.batch_tree.column(key, width=width, anchor="w" if key in ("target", "reason", "path") else "center")
        self.batch_tree.grid(row=0, column=0, sticky="nsew")
        result_scroll = ttk.Scrollbar(result_box, command=self.batch_tree.yview)
        result_scroll.grid(row=0, column=1, sticky="ns")
        self.batch_tree.configure(yscrollcommand=result_scroll.set)

        log_box = ttk.LabelFrame(bottom, text="ログ")
        log_box.grid(row=0, column=1, sticky="nsew")
        log_box.columnconfigure(0, weight=1)
        log_box.rowconfigure(0, weight=1)
        self.log_text = tk.Text(log_box, wrap="none")
        self.log_text.grid(row=0, column=0, sticky="nsew")
        log_scroll = ttk.Scrollbar(log_box, command=self.log_text.yview)
        log_scroll.grid(row=0, column=1, sticky="ns")
        self.log_text.configure(yscrollcommand=log_scroll.set)

        self._execution_disable_widgets = [
            self.parquet_entry,
            self.threshold_entry,
            self.btn_browse_parquet,
            self.btn_scan,
            self.btn_browse_threshold,
            self.station_list,
            self.base_dates_text,
            self.radio_window_3,
            self.radio_window_5,
            self.btn_precheck,
        ]

    def _build_style_tab(self, parent: ttk.Frame) -> None:
        """スタイル調整タブを構築する。"""

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
        self._style_common_controls = []
        for row, field in enumerate(COMMON_STYLE_FIELDS):
            control = self._create_style_control(common_box, row=row, field=field)
            self._style_common_controls.append(control)
        ttk.Label(
            common_box,
            text="※ DPIは出力品質の設定です。プレビュー表示サイズは幅・高さを基準に調整されます。",
            foreground="#475569",
        ).grid(row=len(COMMON_STYLE_FIELDS), column=0, columnspan=2, sticky="w", padx=6, pady=(2, 4))

        self.graph_style_box = ttk.LabelFrame(left, text="グラフ別設定")
        self.graph_style_box.grid(row=2, column=0, sticky="ew", pady=(0, 6))
        self.graph_style_box.columnconfigure(1, weight=1)

        style_btns = ttk.Frame(left)
        style_btns.grid(row=3, column=0, sticky="ew", pady=(0, 4))
        ttk.Button(style_btns, text="フォーム適用", command=self._on_style_form_commit).grid(row=0, column=0, padx=(0, 4))
        ttk.Button(style_btns, text="元に戻す(Ctrl+Z)", command=self._undo_style_change).grid(row=0, column=1, padx=(0, 4))
        ttk.Button(style_btns, text="やり直し(Ctrl+Y)", command=self._redo_style_change).grid(row=0, column=2, padx=(0, 8))
        ttk.Separator(style_btns, orient="vertical").grid(row=0, column=3, sticky="ns", padx=(0, 8))
        ttk.Button(style_btns, text="読込", command=self._load_style_from_file).grid(row=0, column=4, padx=(0, 4))
        ttk.Button(style_btns, text="保存", command=self._save_style_to_file).grid(row=0, column=5, padx=(0, 4))
        ttk.Button(style_btns, text="初期化", command=self._reset_style).grid(row=0, column=6)

        json_box = ttk.LabelFrame(left, text="高度設定(JSON)")
        json_box.grid(row=4, column=0, sticky="nsew")
        json_box.columnconfigure(0, weight=1)
        json_box.rowconfigure(0, weight=1)
        self.style_text = tk.Text(json_box, wrap="none", undo=True, autoseparators=True, maxundo=2000)
        self.style_text.grid(row=0, column=0, sticky="nsew", padx=(6, 0), pady=6)
        json_scroll = ttk.Scrollbar(json_box, command=self.style_text.yview)
        json_scroll.grid(row=0, column=1, sticky="ns", padx=(0, 6), pady=6)
        self.style_text.configure(yscrollcommand=json_scroll.set)
        self._set_style_text_from_payload()
        self.style_text.bind("<KeyRelease>", self._on_style_text_changed)
        self.bind("<Control-z>", self._on_undo_shortcut)
        self.bind("<Control-y>", self._on_redo_shortcut)
        self.bind("<Control-Z>", self._on_redo_shortcut)

        right = ttk.Frame(parent, padding=6)
        right.grid(row=0, column=1, sticky="nsew")
        right.columnconfigure(0, weight=1)
        right.rowconfigure(1, weight=1)

        preview_target = ttk.LabelFrame(right, text="プレビュー対象")
        preview_target.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        ttk.Label(preview_target, text="観測所").grid(row=0, column=0, padx=6, pady=6)
        self.preview_station_combo = ttk.Combobox(
            preview_target,
            textvariable=self.preview_target_station,
            state="readonly",
            width=36,
        )
        self.preview_station_combo.grid(row=0, column=1, padx=6, pady=6)
        ttk.Label(preview_target, text="基準日").grid(row=0, column=2, padx=6, pady=6)
        self.preview_date_combo = ttk.Combobox(
            preview_target,
            textvariable=self.preview_target_date,
            state="readonly",
            width=12,
        )
        self.preview_date_combo.grid(row=0, column=3, padx=6, pady=6)
        ttk.Label(preview_target, text="グラフ").grid(row=0, column=4, padx=6, pady=6)
        self.preview_graph_combo = ttk.Combobox(
            preview_target,
            textvariable=self.preview_target_graph,
            values=list(GRAPH_TYPES),
            state="readonly",
            width=24,
        )
        self.preview_graph_combo.grid(row=0, column=5, padx=6, pady=6)
        self.preview_graph_combo.bind("<<ComboboxSelected>>", self._on_preview_graph_selected)
        ttk.Button(preview_target, text="プレビュー更新", command=self._render_preview).grid(row=0, column=6, padx=6, pady=6)

        preview_box = ttk.LabelFrame(right, text="プレビュー")
        preview_box.grid(row=1, column=0, sticky="nsew")
        preview_box.columnconfigure(0, weight=1)
        preview_box.rowconfigure(0, weight=1)
        self.preview_label = ttk.Label(preview_box, text="プレビュー未生成", anchor="center")
        self.preview_label.grid(row=0, column=0, sticky="nsew", padx=6, pady=6)
        ttk.Label(preview_box, textvariable=self.preview_message, foreground="#475569").grid(
            row=1, column=0, sticky="w", padx=6, pady=(0, 6)
        )
        self._refresh_style_forms_from_payload()

    def _activate_dev_dummy_mode(self) -> None:
        """開発者モード用のダミーカタログを有効化する。"""

        catalog = self._build_dev_dummy_catalog()
        self._catalog = catalog
        self._catalog_stations = catalog.stations
        self._base_dates = catalog.base_dates
        self._precheck_ok_targets = []
        base_date = date(2024, 9, 1)
        for graph_type in GRAPH_TYPES:
            self._precheck_ok_targets.append(
                GraphTarget(
                    source="water_info",
                    station_key="DEV001",
                    graph_type=graph_type,  # type: ignore[arg-type]
                    base_date=base_date if graph_type in {"hyetograph", "hydrograph_discharge", "hydrograph_water_level"} else None,
                    event_window_days=int(self.event_window_days.get()) if graph_type in {"hyetograph", "hydrograph_discharge", "hydrograph_water_level"} else None,
                )
            )
        self._refresh_preview_choices()
        self.preview_message.set("開発者モード: ダミーデータでプレビュー可能です。")
        self._append_log("[DEV] dummy catalog loaded for style preview")

    def _build_dev_dummy_catalog(self) -> ParquetCatalog:
        """スタイル調整用のダミー時系列カタログを作る。"""

        records: list[dict[str, Any]] = []
        source = "water_info"
        station_key = "DEV001"
        station_name = "開発用ダミー観測所"
        metric_units = {
            "rainfall": "mm",
            "water_level": "m",
            "discharge": "m3/s",
        }

        # 5日窓プレビューに使えるよう、連続1時間データを生成する。
        event_index = pd.date_range(
            start="2024-08-30 00:00:00",
            end="2024-09-03 23:00:00",
            freq="1h",
        )
        for metric in ("rainfall", "water_level", "discharge"):
            for i, ts in enumerate(event_index):
                if metric == "rainfall":
                    value = max(0.0, 35.0 * (1.0 - abs(i - 48) / 60.0))
                elif metric == "water_level":
                    value = 2.2 + max(0.0, 2.8 * (1.0 - abs(i - 52) / 72.0))
                else:
                    value = 150.0 + max(0.0, 620.0 * (1.0 - abs(i - 54) / 70.0))
                records.append(
                    {
                        "source": source,
                        "station_key": station_key,
                        "station_name": station_name,
                        "observed_at": ts.to_pydatetime(),
                        "metric": metric,
                        "value": float(value),
                        "unit": metric_units[metric],
                        "interval": "1hour",
                        "quality": "normal",
                    }
                )

        # 年最大系プレビューの要件(10年以上)を満たすため、年ごとのピークを入れる。
        for year in range(2015, 2026):
            for metric in ("rainfall", "water_level", "discharge"):
                if metric == "rainfall":
                    value = 48.0 + (year - 2015) * 2.5
                elif metric == "water_level":
                    value = 3.1 + (year - 2015) * 0.12
                else:
                    value = 240.0 + (year - 2015) * 36.0
                records.append(
                    {
                        "source": source,
                        "station_key": station_key,
                        "station_name": station_name,
                        "observed_at": datetime(year, 9, 1, 12, 0, 0),
                        "metric": metric,
                        "value": float(value),
                        "unit": metric_units[metric],
                        "interval": "1hour",
                        "quality": "normal",
                    }
                )

        frame = pd.DataFrame.from_records(records)
        return ParquetCatalog(data=frame, invalid_files={}, warnings=["dev_dummy_catalog"])

    def _browse_parquet_dir(self) -> None:
        """Parquet ディレクトリをファイルダイアログから選ぶ。"""

        path = filedialog.askdirectory()
        if path:
            self.parquet_dir.set(path)

    def _browse_threshold_file(self) -> None:
        """基準線ファイルを選ぶ。"""

        path = filedialog.askopenfilename(filetypes=[("Threshold files", "*.csv *.json"), ("All files", "*.*")])
        if path:
            self.threshold_path.set(path)

    def _scan_parquet(self) -> None:
        """Parquet 配下を走査して観測所候補を更新する。"""

        if self._scan_running:
            return
        if self._running:
            messagebox.showwarning("実行中", "バッチ実行中はスキャンできません。")
            return
        parquet_dir = self.parquet_dir.get().strip()
        if not parquet_dir:
            messagebox.showerror("入力エラー", "Parquet ディレクトリを指定してください。")
            return
        self._append_log(f"[SCAN] start {parquet_dir}")
        # ディレクトリ走査は重くなりやすいので、UI を止めないよう別スレッドへ逃がす。
        self.batch_status.set("スキャン中...")
        self.precheck_summary.set("スキャン中...")
        self._set_scan_state(True)

        def worker() -> None:
            try:
                catalog = self.service.scan_catalog(parquet_dir)
                self._event_queue.put(("scan_done", catalog))
            except Exception as exc:  # noqa: BLE001
                self._event_queue.put(("scan_error", str(exc)))

        threading.Thread(target=worker, daemon=True).start()

    def _apply_scan_result(self, catalog: ParquetCatalog) -> None:
        # 新しいカタログが入ったら、以前の検証結果は破棄して整合性を保つ。
        self._catalog = catalog
        self._catalog_stations = catalog.stations
        self._base_dates = catalog.base_dates
        self._reset_execution_state_for_new_scan()
        self.station_list.delete(0, "end")
        for source, station_key, station_name in self._catalog_stations:
            label = f"{source}:{station_key} ({station_name})" if station_name else f"{source}:{station_key}"
            self.station_list.insert("end", label)
        self.precheck_summary.set(
            f"スキャン完了: stations={len(self._catalog_stations)} / dates={len(self._base_dates)} / invalid_files={len(catalog.invalid_files)}"
        )
        self.batch_status.set("待機中")
        self._append_log(
            f"[SCAN] done stations={len(self._catalog_stations)} dates={len(self._base_dates)} invalid_files={len(catalog.invalid_files)}"
        )

    def _reset_execution_state_for_new_scan(self) -> None:
        # 元データが変わると検証済み対象も変わるので、結果表示を初期化する。
        self._precheck_ok_targets = []
        self._preview_pending = None
        for item_id in self.precheck_tree.get_children():
            self.precheck_tree.delete(item_id)
        for item_id in self.batch_tree.get_children():
            self.batch_tree.delete(item_id)
        self._clear_preview_choices()

    def _clear_preview_choices(self) -> None:
        # プレビュー候補は前回の選択を残さない。
        self.preview_station_combo.configure(values=())
        self.preview_date_combo.configure(values=())
        self.preview_graph_combo.configure(values=())
        self.preview_target_station.set("")
        self.preview_target_date.set("")
        self.preview_target_graph.set(GRAPH_TYPES[0])
        self.preview_message.set("")
        self._preview_photo = None
        self.preview_label.configure(image="", text="プレビュー未生成")
        self._refresh_style_forms_from_payload()

    def _set_scan_state(self, running: bool) -> None:
        # スキャン中は入力変更と実行を止めて、状態競合を避ける。
        self._scan_running = running
        state = "disabled" if running else "normal"
        for widget in self._execution_disable_widgets:
            try:
                widget.configure(state=state)
            except tk.TclError:
                pass
        for chk in self._graph_type_checkbuttons:
            chk.configure(state=state)
        self.run_btn.configure(state="disabled" if running else "normal")
        self.stop_btn.configure(state="disabled")

    def _run_precheck(self) -> None:
        """選択条件に対して実行前検証を行う。"""

        if self._scan_running:
            messagebox.showwarning("スキャン中", "スキャン完了後に実行前検証を行ってください。")
            return
        graph_types = [g for g, var in self.graph_type_vars.items() if var.get()]
        if not graph_types:
            messagebox.showerror("入力エラー", "グラフ種別を1つ以上選択してください。")
            return
        selected_indices = self.station_list.curselection()
        if not selected_indices:
            messagebox.showerror("入力エラー", "観測所を1つ以上選択してください。")
            return
        station_pairs: list[tuple[str, str]] = []
        seen_pairs: set[tuple[str, str]] = set()
        for idx in selected_indices:
            source, station_key, _name = self._catalog_stations[int(idx)]
            pair = (source, station_key)
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)
            station_pairs.append(pair)

        # 表示上は source と station_key を組にして、同名観測所や別ソースの衝突を避ける。
        base_dates = [line.strip() for line in self.base_dates_text.get("1.0", "end").splitlines() if line.strip()]
        precheck_input = PrecheckInput(
            parquet_dir=self.parquet_dir.get().strip(),
            threshold_file_path=self.threshold_path.get().strip() or None,
            graph_types=graph_types,
            station_pairs=station_pairs,
            base_dates=base_dates,
            event_window_days=int(self.event_window_days.get()),
        )
        self._append_log(
            f"[PRECHECK] start stations={len(station_pairs)} graph_types={len(graph_types)} base_dates={len(base_dates)}"
        )
        threshold_file = self.threshold_path.get().strip() or None
        threshold_result = self._load_thresholds_cached(threshold_file)
        if self._catalog is not None:
            result = self.service.precheck_with_catalog(
                catalog=self._catalog,
                data=precheck_input,
                threshold_result=threshold_result,
            )
        else:
            result = self.service.precheck(precheck_input)

        self._precheck_ok_targets = []
        for item_id in self.precheck_tree.get_children():
            self.precheck_tree.delete(item_id)
        for row in result.items:
            reason = row.reason_message or ""
            self.precheck_tree.insert("", "end", values=(row.target_id, row.status, reason))
            if row.status == "ok":
                base = date.fromisoformat(row.base_datetime) if row.base_datetime else None
                self._precheck_ok_targets.append(
                    GraphTarget(
                        source=row.source,
                        station_key=row.station_key,
                        graph_type=row.graph_type,
                        base_date=base,
                        event_window_days=int(self.event_window_days.get()) if base else None,
                    )
                )
        self.precheck_summary.set(
            f"対象数: {result.summary.total_targets} / OK: {result.summary.ok_targets} / NG: {result.summary.ng_targets}"
        )
        self._append_log(
            f"[PRECHECK] done total={result.summary.total_targets} ok={result.summary.ok_targets} ng={result.summary.ng_targets}"
        )
        self._refresh_preview_choices()

    def _refresh_preview_choices(self) -> None:
        """プレビューで選べる対象候補を更新する。"""

        if not self._precheck_ok_targets:
            self._clear_preview_choices()
            return
        # 実行前検証を通過した対象だけを候補に出すと、プレビュー失敗を減らせる。
        station_values = sorted({f"{t.source}:{t.station_key}" for t in self._precheck_ok_targets})
        date_values = sorted({t.base_date.isoformat() for t in self._precheck_ok_targets if t.base_date is not None})
        graph_values = sorted({t.graph_type for t in self._precheck_ok_targets})
        self.preview_station_combo.configure(values=station_values)
        self.preview_date_combo.configure(values=date_values)
        self.preview_graph_combo.configure(values=graph_values)
        if not self.preview_target_station.get() and station_values:
            self.preview_target_station.set(station_values[0])
        if not self.preview_target_date.get() and date_values:
            self.preview_target_date.set(date_values[0])
        if self.preview_target_graph.get() not in graph_values and graph_values:
            self.preview_target_graph.set(graph_values[0])
        self._refresh_style_forms_from_payload()

    def _create_style_control(self, parent: ttk.Frame, *, row: int, field: dict[str, Any]) -> dict[str, Any]:
        """スタイルフォームの 1 項目を構築して制御情報を返す。"""

        kind = str(field.get("kind", "str"))
        label = str(field.get("label", ""))
        path = str(field.get("path", ""))
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky="w", padx=6, pady=3)

        if kind == "bool":
            var: tk.Variable = tk.BooleanVar(value=False)
            widget = ttk.Checkbutton(parent, variable=var, command=self._on_style_form_commit)
            widget.grid(row=row, column=1, sticky="w", padx=6, pady=3)
        elif kind == "choice":
            var = tk.StringVar(value="")
            widget = ttk.Combobox(
                parent,
                textvariable=var,
                state="readonly",
                values=tuple(field.get("values") or ()),
                width=30,
            )
            widget.grid(row=row, column=1, sticky="ew", padx=6, pady=3)
            widget.bind("<<ComboboxSelected>>", self._on_style_form_commit_event)
            widget.bind("<Return>", self._on_style_form_commit_event)
        else:
            var = tk.StringVar(value="")
            widget = ttk.Entry(parent, textvariable=var)
            widget.grid(row=row, column=1, sticky="ew", padx=6, pady=3)
            widget.bind("<Return>", self._on_style_form_commit_event)

        return {"path": path, "label": label, "kind": kind, "var": var, "widget": widget}

    def _current_style_graph_type(self) -> str:
        """スタイル編集対象のグラフ種別を返す。"""

        graph_type = self.preview_target_graph.get().strip()
        if graph_type in GRAPH_TYPES:
            return graph_type
        return GRAPH_TYPES[0]

    def _graph_style_fields_for(self, graph_style: dict[str, Any]) -> list[dict[str, Any]]:
        """対象グラフに応じた編集項目定義を返す。"""

        fields = [dict(field) for field in BASE_GRAPH_STYLE_FIELDS]
        if self._nested_value(graph_style, "x_axis.tick_interval_hours", None) is not None:
            fields.append({"path": "x_axis.tick_interval_hours", "label": "X軸間隔(時間)", "kind": "int"})
        if self._nested_value(graph_style, "bar_color", None) is not None:
            fields.append({"path": "bar_color", "label": "棒色", "kind": "str"})
        if self._nested_value(graph_style, "bar.width", None) is not None:
            fields.append({"path": "bar.width", "label": "棒幅", "kind": "float"})
        if self._nested_value(graph_style, "invert_y_axis", None) is not None:
            fields.append({"path": "invert_y_axis", "label": "Y軸反転", "kind": "bool"})
        if self._nested_value(graph_style, "threshold.label_enabled", None) is not None:
            fields.append({"path": "threshold.label_enabled", "label": "基準線ラベル表示", "kind": "bool"})
        return fields

    def _refresh_style_forms_from_payload(self) -> None:
        """現在のスタイル payload からフォーム表示を更新する。"""

        self._style_form_updating = True
        try:
            common = self._style_payload.get("common", {})
            for control in self._style_common_controls:
                value = self._nested_value(common, control["path"], None)
                self._set_control_var(control, value)

            graph_type = self._current_style_graph_type()
            graph_styles = self._style_payload.setdefault("graph_styles", {})
            graph_style = graph_styles.get(graph_type)
            if not isinstance(graph_style, dict):
                graph_style = {}
                graph_styles[graph_type] = graph_style
            self.graph_style_box.configure(text=f"グラフ別設定 ({GRAPH_TYPE_LABELS.get(graph_type, graph_type)})")
            for child in self.graph_style_box.winfo_children():
                child.destroy()
            self.graph_style_box.columnconfigure(1, weight=1)
            self._style_graph_controls = []
            for row, field in enumerate(self._graph_style_fields_for(graph_style)):
                control = self._create_style_control(self.graph_style_box, row=row, field=field)
                value = self._nested_value(graph_style, control["path"], None)
                self._set_control_var(control, value)
                self._style_graph_controls.append(control)
        finally:
            self._style_form_updating = False

    def _set_style_text_from_payload(self) -> None:
        """payload を JSON エディタへ反映する。"""

        self._style_text_syncing = True
        try:
            self.style_text.delete("1.0", "end")
            self.style_text.insert("1.0", json.dumps(self._style_payload, ensure_ascii=False, indent=2))
        finally:
            self._style_text_syncing = False

    def _set_control_var(self, control: dict[str, Any], value: Any) -> None:
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

    def _on_preview_graph_selected(self, _event=None) -> None:
        """プレビュー対象グラフの変更に合わせて編集フォームを切り替える。"""

        self._refresh_style_forms_from_payload()
        self._schedule_preview_refresh()

    def _on_style_text_changed(self, _event=None) -> None:
        """JSON 直接編集時に payload とフォームを同期する。"""

        if self._style_text_syncing:
            return
        payload = self._style_from_editor(silent=True)
        if payload is not None:
            self._style_payload = payload
            self._refresh_style_forms_from_payload()
        self._schedule_preview_refresh()

    def _on_style_form_commit_event(self, event=None) -> str | None:
        """Enter/選択確定イベントからフォーム適用を呼ぶ。"""

        self._on_style_form_commit()
        if event is not None and getattr(event, "keysym", "") == "Return":
            return "break"
        return None

    def _on_style_form_commit(self) -> None:
        """フォーム編集内容を確定反映する。"""

        if self._style_form_updating:
            return
        payload = self._style_from_editor(silent=True)
        if payload is not None:
            self._style_payload = payload
        if not self._apply_style_form_values():
            return
        self._set_style_text_from_payload()
        self._push_style_history(self._style_payload)
        self._render_preview(silent_json_error=True)

    def _apply_style_form_values(self) -> bool:
        """フォームの値を payload に反映する。"""

        common = self._style_payload.setdefault("common", {})
        for control in self._style_common_controls:
            current_value = self._nested_value(common, control["path"], None)
            value, error = self._coerce_control_value(control, current_value)
            if error:
                self.preview_message.set(error)
                return False
            self._set_nested_value(common, control["path"], value)

        graph_type = self._current_style_graph_type()
        graph_styles = self._style_payload.setdefault("graph_styles", {})
        graph_style = graph_styles.get(graph_type)
        if not isinstance(graph_style, dict):
            graph_style = {}
            graph_styles[graph_type] = graph_style
        for control in self._style_graph_controls:
            current_value = self._nested_value(graph_style, control["path"], None)
            value, error = self._coerce_control_value(control, current_value)
            if error:
                self.preview_message.set(error)
                return False
            self._set_nested_value(graph_style, control["path"], value)
        return True

    def _coerce_control_value(self, control: dict[str, Any], current_value: Any) -> tuple[Any, str | None]:
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

    def _push_style_history(self, payload: dict[str, Any]) -> None:
        """履歴スタックへ現在スタイルを積む。"""

        if self._style_history_applying:
            return
        snapshot = deepcopy(payload)
        if self._style_history and snapshot == self._style_history[self._style_history_index]:
            return
        if self._style_history_index < len(self._style_history) - 1:
            self._style_history = self._style_history[: self._style_history_index + 1]
        self._style_history.append(snapshot)
        if len(self._style_history) > 200:
            drop = len(self._style_history) - 200
            self._style_history = self._style_history[drop:]
            self._style_history_index = max(0, self._style_history_index - drop)
        self._style_history_index = len(self._style_history) - 1

    def _apply_style_snapshot(self, payload: dict[str, Any]) -> None:
        """履歴スナップショットを UI に反映する。"""

        self._style_history_applying = True
        try:
            self._style_payload = deepcopy(payload)
            self._set_style_text_from_payload()
            self._refresh_style_forms_from_payload()
        finally:
            self._style_history_applying = False
        self._render_preview(silent_json_error=True)

    def _undo_style_change(self) -> None:
        """スタイル変更を 1 段戻す。"""

        if self._style_history_index <= 0:
            self.preview_message.set("これ以上戻せません。")
            return
        self._style_history_index -= 1
        self._apply_style_snapshot(self._style_history[self._style_history_index])

    def _redo_style_change(self) -> None:
        """取り消したスタイル変更を 1 段進める。"""

        if self._style_history_index >= len(self._style_history) - 1:
            self.preview_message.set("これ以上進められません。")
            return
        self._style_history_index += 1
        self._apply_style_snapshot(self._style_history[self._style_history_index])

    def _on_undo_shortcut(self, _event=None) -> str | None:
        """Ctrl+Z で履歴を戻す。"""

        if self.notebook.select() != str(self.style_tab):
            return None
        focus = self.focus_get()
        if isinstance(focus, tk.Text):
            return None
        self._undo_style_change()
        return "break"

    def _on_redo_shortcut(self, _event=None) -> str | None:
        """Ctrl+Y / Ctrl+Shift+Z で履歴を進める。"""

        if self.notebook.select() != str(self.style_tab):
            return None
        focus = self.focus_get()
        if isinstance(focus, tk.Text):
            return None
        self._redo_style_change()
        return "break"

    def _nested_value(self, root: dict[str, Any], path: str, default: Any) -> Any:
        """ドット区切りパスから値を取得する。"""

        node: Any = root
        for key in path.split("."):
            if not isinstance(node, dict) or key not in node:
                return default
            node = node[key]
        return node

    def _set_nested_value(self, root: dict[str, Any], path: str, value: Any) -> None:
        """ドット区切りパスへ値を書き込む。"""

        parts = path.split(".")
        node: dict[str, Any] = root
        for key in parts[:-1]:
            child = node.get(key)
            if not isinstance(child, dict):
                child = {}
                node[key] = child
            node = child
        node[parts[-1]] = value

    def _load_style_from_file(self) -> None:
        """スタイル JSON を読込む。"""

        path = filedialog.askopenfilename(filetypes=[("JSON", "*.json"), ("All", "*.*")])
        if not path:
            return
        self._style_json_path = path
        try:
            payload = json.loads(Path(path).read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("読込エラー", str(exc))
            return
        self._style_payload = payload
        self._set_style_text_from_payload()
        self._refresh_style_forms_from_payload()
        self._push_style_history(self._style_payload)
        self._append_log(f"[STYLE] loaded {path}")
        self._render_preview()

    def _save_style_to_file(self) -> None:
        """現在のスタイルを JSON として保存する。"""

        path = filedialog.asksaveasfilename(defaultextension=".json", filetypes=[("JSON", "*.json"), ("All", "*.*")])
        if not path:
            return
        payload = self._style_from_editor()
        if payload is None:
            return
        try:
            save_style(path, payload)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("保存エラー", str(exc))
            return
        self._style_json_path = path
        self._append_log(f"[STYLE] saved {path}")
        messagebox.showinfo("保存", f"スタイルを保存しました。\n{path}")

    def _reset_style(self) -> None:
        """スタイルを既定値に戻す。"""

        self._style_payload = default_style()
        self._set_style_text_from_payload()
        self._refresh_style_forms_from_payload()
        self._push_style_history(self._style_payload)
        self._append_log("[STYLE] reset to default")
        self._render_preview()

    def _style_from_editor(self, *, silent: bool = False) -> dict | None:
        """スタイルエディタの JSON を辞書へ変換する。"""

        text = self.style_text.get("1.0", "end").strip()
        if not text:
            if silent:
                self.preview_message.set("スタイルJSONが空です。")
            else:
                messagebox.showerror("入力エラー", "スタイルJSONが空です。")
            return None
        try:
            return json.loads(text)
        except Exception as exc:  # noqa: BLE001
            if silent:
                self.preview_message.set(f"JSONエラー: {exc}")
            else:
                messagebox.showerror("JSONエラー", str(exc))
            return None

    def _schedule_preview_refresh(self, _event=None) -> None:
        """入力のたびに即時再描画しすぎないよう少し遅延させる。"""

        if self._style_debounce_id:
            self.after_cancel(self._style_debounce_id)
        self._style_debounce_id = self.after(1200, lambda: self._render_preview(silent_json_error=True))

    def _render_preview(self, *, silent_json_error: bool = False) -> None:
        """プレビュー画像を再生成して表示する。"""

        if self._scan_running:
            return
        if self._catalog is None:
            self.preview_message.set("先にParquetをスキャンしてください。")
            return
        station_token = self.preview_target_station.get().strip()
        graph_type = self.preview_target_graph.get().strip()
        if not station_token or not graph_type:
            return
        try:
            source, station_key = station_token.split(":", 1)
        except ValueError:
            self.preview_message.set("観測所の指定が不正です。")
            return
        base_date = self.preview_target_date.get().strip() or None
        payload = self._style_from_editor(silent=silent_json_error)
        if payload is None:
            return
        self._style_payload = payload
        preview_payload = self._build_preview_style_payload(payload)
        threshold_file = self.threshold_path.get().strip() or None
        preview_input = PreviewInput(
            parquet_dir=self.parquet_dir.get().strip(),
            threshold_file_path=threshold_file,
            style_json_path=self._style_json_path,
            style_payload=preview_payload,
            source=source,
            station_key=station_key,
            graph_type=graph_type,
            base_datetime=base_date,
            event_window_days=int(self.event_window_days.get()) if base_date else None,
        )
        self._preview_pending = {"input": preview_input, "threshold_file": threshold_file}
        self.preview_message.set("プレビュー更新中...")
        self._start_preview_worker_if_needed()

    def _build_preview_style_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        """プレビュー用にスタイルを調整して返す。"""

        preview_payload = deepcopy(payload)
        common = preview_payload.setdefault("common", {})
        # DPIは出力品質にのみ効かせ、プレビュー表示サイズは幅・高さの変更を主軸にする。
        common["dpi"] = 120
        return preview_payload

    def _start_preview_worker_if_needed(self) -> None:
        """最新のプレビュー要求だけをバックグラウンドで実行する。"""

        if self._preview_running:
            return
        if self._preview_pending is None or self._catalog is None:
            return

        pending = self._preview_pending
        self._preview_pending = None
        preview_input: PreviewInput = pending["input"]
        threshold_file: str | None = pending["threshold_file"]
        catalog = self._catalog
        threshold_result = self._load_thresholds_cached(threshold_file)
        self._preview_running = True

        def worker() -> None:
            try:
                result = self.service.preview_with_catalog(
                    catalog=catalog,
                    data=preview_input,
                    threshold_result=threshold_result,
                )
                self._event_queue.put(("preview_done", result))
            except Exception as exc:  # noqa: BLE001
                self._event_queue.put(("preview_error", str(exc)))

        threading.Thread(target=worker, daemon=True).start()

    def _load_thresholds_cached(self, threshold_file_path: str | None) -> ThresholdLoadResult | None:
        """しきい値ファイルの読込結果をキャッシュして使い回す。"""

        if not threshold_file_path:
            return None
        path = Path(threshold_file_path)
        try:
            mtime_ns = path.stat().st_mtime_ns
        except OSError:
            return load_thresholds(threshold_file_path)
        if (
            self._threshold_cache_path == str(path)
            and self._threshold_cache_mtime_ns == mtime_ns
            and self._threshold_cache_result is not None
        ):
            return self._threshold_cache_result
        result = load_thresholds(threshold_file_path)
        self._threshold_cache_path = str(path)
        self._threshold_cache_mtime_ns = mtime_ns
        self._threshold_cache_result = result
        return result

    def _start_batch_run(self) -> None:
        """バッチ実行を開始する。"""

        if self._running:
            return
        if self._scan_running:
            messagebox.showwarning("スキャン中", "スキャン完了後にバッチ実行してください。")
            return
        if not self._precheck_ok_targets:
            messagebox.showwarning("未検証", "実行前検証でOK対象を作成してください。")
            return
        out_dir = filedialog.askdirectory(title="出力先フォルダを選択")
        if not out_dir:
            return
        payload = self._style_from_editor()
        if payload is None:
            return
        self._style_payload = payload
        # 検証済み対象だけを BatchTarget に落とし込む。
        batch_targets = [
            BatchTarget(
                source=t.source,
                station_key=t.station_key,
                graph_type=t.graph_type,
                base_datetime=t.base_date.isoformat() if t.base_date else None,
                event_window_days=t.event_window_days,
            )
            for t in self._precheck_ok_targets
        ]
        run_input = BatchRunInput(
            parquet_dir=self.parquet_dir.get().strip(),
            output_dir=out_dir,
            threshold_file_path=self.threshold_path.get().strip() or None,
            style_json_path=self._style_json_path,
            style_payload=payload,
            targets=batch_targets,
            should_stop=self._stop_event.is_set if self._stop_event else None,
        )
        for item_id in self.batch_tree.get_children():
            self.batch_tree.delete(item_id)
        self._stop_event = threading.Event()
        self._set_running_state(True)
        self.batch_status.set("実行中...")
        self._append_log(f"[RUN] start targets={len(batch_targets)} out={out_dir}")

        def worker() -> None:
            try:
                result = self.service.run_batch(
                    run_input,
                    stop_requested=self._stop_event.is_set if self._stop_event else None,
                )
                self._event_queue.put(("run_done", result))
            except Exception as exc:  # noqa: BLE001
                self._event_queue.put(("run_error", str(exc)))

        threading.Thread(target=worker, daemon=True).start()

    def _request_stop(self) -> None:
        """バッチ停止を要求する。"""

        if self._stop_event is not None:
            self._stop_event.set()
            self.batch_status.set("停止要求中...")
            self._append_log("[RUN] stop requested")

    def _set_running_state(self, running: bool) -> None:
        """実行中かどうかに応じて UI をロック/解除する。"""

        self._running = running
        # 実行中は入力面を触れないようにして、結果表示に集中させる。
        self.run_btn.configure(state="disabled" if running else "normal")
        self.stop_btn.configure(state="normal" if running else "disabled")
        for widget in self._execution_disable_widgets:
            try:
                widget.configure(state="disabled" if running else "normal")
            except tk.TclError:
                pass
        for chk in self._graph_type_checkbuttons:
            chk.configure(state="disabled" if running else "normal")
        if running:
            # スタイル変更が実行結果に影響するので、実行中はタブ切替も抑止する。
            self.notebook.tab(self.style_tab, state="disabled")
        else:
            self.notebook.tab(self.style_tab, state="normal")

    def _poll_events(self) -> None:
        """ワーカースレッドの結果をメインスレッドで反映する。"""

        try:
            while True:
                event, payload = self._event_queue.get_nowait()
                if event == "scan_done":
                    self._set_scan_state(False)
                    self._apply_scan_result(payload)
                elif event == "scan_error":
                    self._set_scan_state(False)
                    self.batch_status.set("待機中")
                    self.precheck_summary.set("対象数: 0 / NG: 0")
                    self._append_log(f"[SCAN] error {payload}")
                    messagebox.showerror("読込エラー", str(payload))
                elif event == "run_done":
                    result = payload
                    # バッチの結果は 1 件ずつ表に積み上げて、途中経過を追いやすくする。
                    for row in result.items:
                        self.batch_tree.insert(
                            "",
                            "end",
                            values=(row.target_id, row.status, row.reason_message or "", row.output_path or ""),
                        )
                    self.batch_status.set(
                        f"完了: success={result.summary.success}, failed={result.summary.failed}, skipped={result.summary.skipped}"
                    )
                    self._append_log(
                        f"[RUN] done success={result.summary.success} failed={result.summary.failed} skipped={result.summary.skipped}"
                    )
                    self._set_running_state(False)
                elif event == "run_error":
                    self.batch_status.set("エラー")
                    self._append_log(f"[RUN] error {payload}")
                    messagebox.showerror("バッチ実行エラー", str(payload))
                    self._set_running_state(False)
                elif event == "preview_done":
                    result = payload
                    self._preview_running = False
                    if result.status != "success" or result.image_bytes_png is None:
                        self.preview_message.set(result.reason_message or "プレビュー生成に失敗しました。")
                    else:
                        encoded = base64.b64encode(result.image_bytes_png).decode("ascii")
                        self._preview_photo = tk.PhotoImage(data=encoded)
                        self.preview_label.configure(image=self._preview_photo, text="")
                        self.preview_message.set("プレビュー更新完了")
                    self._start_preview_worker_if_needed()
                elif event == "preview_error":
                    self._preview_running = False
                    self.preview_message.set(str(payload))
                    self._start_preview_worker_if_needed()
        except queue.Empty:
            pass
        self.after(120, self._poll_events)

    def _append_log(self, message: str) -> None:
        """ログ欄に 1 行追加する。"""

        self.log_text.insert("end", message.rstrip() + "\n")
        self.log_text.see("end")

    def _open_other(self, app_key: str) -> None:
        """別アプリへ遷移する。"""

        if self.on_open_other:
            self.destroy()
            self.on_open_other(app_key)

    def _on_close(self) -> None:
        """ウィンドウ終了時の後始末を行う。"""

        if self._running:
            if not messagebox.askyesno("終了確認", "実行中です。停止して終了しますか？"):
                return
            if self._stop_event:
                self._stop_event.set()
        try:
            self.destroy()
        finally:
            if self.on_close:
                self.on_close()

    def _return_home(self) -> None:
        """ホーム画面へ戻る。"""

        try:
            self.destroy()
        finally:
            if self.on_return_home:
                self.on_return_home()


def show_hydrology_graphs(
    *,
    parent: tk.Misc,
    on_open_other=None,
    on_close=None,
    on_return_home=None,
    developer_mode: bool = False,
) -> HydrologyGraphsApp:
    """Hydrology Graphs 画面を生成して返す。"""

    return HydrologyGraphsApp(
        parent=parent,
        on_open_other=on_open_other,
        on_close=on_close,
        on_return_home=on_return_home,
        developer_mode=developer_mode,
    )


def main() -> int:
    """単独起動用のエントリポイント。"""

    root = tk.Tk()
    root.withdraw()

    def _on_close() -> None:
        root.destroy()

    show_hydrology_graphs(parent=root, on_close=_on_close)
    root.mainloop()
    return 0
