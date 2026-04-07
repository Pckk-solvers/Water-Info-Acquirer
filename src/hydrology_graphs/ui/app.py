from __future__ import annotations

import json
import queue
import threading
import tkinter as tk
import tkinter.font as tkfont
from copy import deepcopy
from datetime import date, datetime
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Any

import pandas as pd

from hydrology_graphs.domain.constants import GRAPH_TYPES
from hydrology_graphs.domain.models import GraphTarget
from hydrology_graphs.io.parquet_store import ParquetCatalog
from hydrology_graphs.io.style_store import (
    STYLE_GRAPH_KEYS,
    VALID_TIME_DISPLAY_MODES,
    default_style,
    load_style,
    save_style,
)
from hydrology_graphs.io.threshold_store import ThresholdCacheState, ThresholdLoadResult, load_thresholds_with_cache
from hydrology_graphs.services import HydrologyGraphService, PreviewInput
from water_info_acquirer.app_meta import get_module_title
from water_info_acquirer.navigation import build_navigation_menu
from .event_handlers import handle_event
from .execute_actions import (
    add_base_date_from_candidate,
    clear_base_dates,
    export_base_dates_csv,
    import_base_dates_csv,
    refresh_preview_choices,
    remove_selected_base_dates,
    run_precheck,
    start_batch_run,
)
from .preview_canvas import (
    current_preview_canvas_size,
    display_preview_image,
    draw_preview_photo,
    on_preview_area_resized,
    show_preview_placeholder,
)
from .preview_actions import export_preview_sample, render_preview
from .style_payload import nested_value, set_nested_value
from .tabs_execute import build_execute_tab
from .tabs_style import build_style_tab
from .view_models import format_station_display_text
from .tooltip import ToolTip

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

SOURCE_LABELS = {
    "jma": "気象庁",
    "water_info": "水文水質DB",
}

STYLE_TARGET_LABELS: dict[str, str] = {
    "hyetograph:3day": "ハイエトグラフ（雨量） 3日",
    "hyetograph:5day": "ハイエトグラフ（雨量） 5日",
    "hydrograph_discharge:3day": "ハイドログラフ（流量） 3日",
    "hydrograph_discharge:5day": "ハイドログラフ（流量） 5日",
    "hydrograph_water_level:3day": "ハイドログラフ（水位） 3日",
    "hydrograph_water_level:5day": "ハイドログラフ（水位） 5日",
    "annual_max_rainfall": "年最大雨量",
    "annual_max_discharge": "年最大流量",
    "annual_max_water_level": "年最高水位",
}

STYLE_TARGET_ORDER: tuple[str, ...] = (
    "hyetograph:3day",
    "hyetograph:5day",
    "hydrograph_discharge:3day",
    "hydrograph_discharge:5day",
    "hydrograph_water_level:3day",
    "hydrograph_water_level:5day",
    "annual_max_rainfall",
    "annual_max_discharge",
    "annual_max_water_level",
)

BASE_GRAPH_STYLE_FIELDS: tuple[dict[str, Any], ...] = (
    {"path": "figure_width", "label": "図幅(inch)", "kind": "float", "tooltip": "出力画像の横幅（inch）。画像サイズの基準になります。"},
    {"path": "figure_height", "label": "図高(inch)", "kind": "float", "tooltip": "出力画像の縦幅（inch）。画像サイズの基準になります。"},
    {"path": "dpi", "label": "DPI", "kind": "int", "tooltip": "解像度（密度）。値を上げると同じ図幅・図高でも高精細になります。"},
    {"path": "font_family", "label": "フォント", "kind": "str"},
    {"path": "font_size", "label": "基本フォントサイズ", "kind": "int"},
    {"path": "background_color", "label": "背景色(#RRGGBB)", "kind": "str"},
    {"path": "legend.enabled", "label": "凡例表示", "kind": "bool", "tooltip": "系列名・基準線ラベルの凡例を表示/非表示にします。"},
    {"path": "grid.enabled", "label": "グリッド表示", "kind": "bool"},
    {"path": "title.template", "label": "タイトルテンプレート", "kind": "str"},
    {"path": "axis.x_label", "label": "X軸ラベル", "kind": "str"},
    {"path": "axis.y_label", "label": "Y軸ラベル", "kind": "str"},
    {"path": "series_color", "label": "系列色", "kind": "str"},
    {"path": "series_width", "label": "系列幅", "kind": "float", "tooltip": "折れ線/系列の太さです。"},
    {
        "path": "series_style",
        "label": "系列線種",
        "kind": "choice",
        "values": ("solid", "dashed", "dashdot", "dotted"),
        "tooltip": "折れ線の線種です。",
    },
    {"path": "x_axis.tick_rotation", "label": "X軸角度", "kind": "float"},
    {"path": "y_axis.tick_count", "label": "Y軸目盛数", "kind": "int", "tooltip": "Y軸の目盛り分割数の目安です。"},
    {
        "path": "y_axis.number_format",
        "label": "Y軸数値形式",
        "kind": "choice",
        "values": ("plain", "comma", "percent"),
    },
)


class HydrologyGraphsApp(tk.Toplevel):
    """水文グラフ生成のメインウィンドウ。"""

    GRAPH_TYPE_LABELS = GRAPH_TYPE_LABELS
    GRAPH_TYPES = GRAPH_TYPES
    SOURCE_LABELS = SOURCE_LABELS
    STYLE_TARGET_LABELS = STYLE_TARGET_LABELS
    STYLE_TARGET_ORDER = STYLE_TARGET_ORDER

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
        self.event_window_3 = tk.BooleanVar(value=True)
        self.event_window_5 = tk.BooleanVar(value=False)
        self.batch_status = tk.StringVar(value="待機中")
        self.preview_message = tk.StringVar(value="")
        self.preview_target_station = tk.StringVar(value="")
        self.preview_target_date = tk.StringVar(value="")
        self.preview_target_graph = tk.StringVar(value="")
        self.time_display_mode = tk.StringVar(value="datetime")
        self.base_date_year = tk.StringVar(value="")
        self.base_date_month = tk.StringVar(value="")
        self.base_date_candidate = tk.StringVar(value="")
        self._preview_station_display_to_pair: dict[str, tuple[str, str]] = {}
        self._preview_graph_display_to_key: dict[str, str] = {}
        self._preview_graph_key_to_display: dict[str, str] = {
            key: STYLE_TARGET_LABELS.get(key, key) for key in STYLE_TARGET_ORDER
        }

        # 画面全体で共有する状態はここで初期化する。
        self._style_json_path: str | None = None
        self._style_payload: dict = default_style()
        self._style_debounce_id: str | None = None
        self._style_text_syncing = False
        self._style_form_updating = False
        self._style_graph_controls: list[dict[str, Any]] = []
        self._style_field_tooltips: list[ToolTip] = []
        self._execute_tooltips: list[ToolTip] = []
        self.display_mode_box: ttk.LabelFrame | None = None
        self._style_history: list[dict[str, Any]] = [deepcopy(self._style_payload)]
        self._style_history_index: int = 0
        self._style_history_applying = False
        self._preview_photo: tk.PhotoImage | None = None
        self._preview_image_bytes: bytes | None = None
        self._preview_canvas_image_id: int | None = None
        self._preview_placeholder_id: int | None = None
        self._preview_last_canvas_size: tuple[int, int] | None = None
        self._preview_last_fit_size: tuple[int, int] | None = None
        self._preview_last_image_hash: int | None = None
        self._catalog: ParquetCatalog | None = None
        self._catalog_stations: list[tuple[str, str, str]] = []
        self._station_metric_labels: dict[tuple[str, str], tuple[str, ...]] = {}
        self._station_row_pairs: list[tuple[str, str] | None] = []
        self._checked_station_pairs: set[tuple[str, str]] = set()
        self._station_selection_dirty = False
        self._base_dates: list[str] = []
        self._base_date_year_to_months: dict[str, list[str]] = {}
        self._base_date_year_month_to_days: dict[tuple[str, str], list[str]] = {}
        self.selected_base_dates: list[str] = []
        self._precheck_ok_targets: list[GraphTarget] = []
        self._result_row_ids: dict[str, str] = {}
        self._result_output_paths: dict[str, str] = {}
        self._event_queue: queue.Queue[tuple[str, object]] = queue.Queue()
        self._running = False
        self._scan_running = False
        self._preview_running = False
        self._preview_pending: dict[str, Any] | None = None
        self._threshold_cache = ThresholdCacheState()
        self._stop_event: threading.Event | None = None
        self._execution_disable_widgets: list[tk.Widget] = []
        self._graph_type_checkbuttons: list[ttk.Checkbutton] = []
        self.event_window_3.trace_add("write", self._on_event_window_days_changed)
        self.event_window_5.trace_add("write", self._on_event_window_days_changed)
        self.time_display_mode.trace_add("write", self._on_time_display_mode_changed)

        self.config(
            menu=build_navigation_menu(
                self,
                current_app_key="hydrology_graphs",
                on_open_other=self._open_other,
                on_return_home=self._return_home,
            )
        )
        self._build_ui()
        show_preview_placeholder(self, "プレビュー未生成")
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
        build_execute_tab(self, parent)

    def _build_style_tab(self, parent: ttk.Frame) -> None:
        """スタイル調整タブを構築する。"""
        build_style_tab(self, parent)

    def _activate_dev_dummy_mode(self) -> None:
        """開発者モード用のダミーカタログを有効化する。"""

        catalog = self._build_dev_dummy_catalog()
        self._catalog = catalog
        self._catalog_stations = catalog.stations
        self._station_metric_labels = dict(catalog.station_metric_labels)
        self._render_station_check_list()
        self._select_all_stations()
        self.selected_base_dates = [self._base_dates[2] if len(self._base_dates) > 2 else self._base_dates[0]] if self._base_dates else []
        if hasattr(self, "base_date_list"):
            self.base_date_list.delete(0, "end")
            for value in self.selected_base_dates:
                self.base_date_list.insert("end", value)
        self._precheck_ok_targets = []
        base_date = date(2024, 9, 1)
        event_windows = self._current_event_window_days_list() or [3]
        for graph_type in GRAPH_TYPES:
            if graph_type in {"hyetograph", "hydrograph_discharge", "hydrograph_water_level"}:
                for window_days in event_windows:
                    self._precheck_ok_targets.append(
                        GraphTarget(
                            source="water_info",
                            station_key="DEV001",
                            graph_type=graph_type,  # type: ignore[arg-type]
                            base_date=base_date,
                            event_window_days=window_days,
                        )
                    )
                continue
            self._precheck_ok_targets.append(
                GraphTarget(
                    source="water_info",
                    station_key="DEV001",
                    graph_type=graph_type,  # type: ignore[arg-type]
                    base_date=None,
                    event_window_days=None,
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
        return ParquetCatalog(
            data=frame,
            invalid_files={},
            warnings=["dev_dummy_catalog"],
            station_metric_labels={("water_info", "DEV001"): ("雨量", "流量", "水位")},
        )

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
                catalog = self.service.scan_station_index(parquet_dir)
                self._event_queue.put(("scan_done", catalog))
            except Exception as exc:  # noqa: BLE001
                self._event_queue.put(("scan_error", str(exc)))

        threading.Thread(target=worker, daemon=True).start()

    def _apply_scan_result(self, catalog: ParquetCatalog) -> None:
        # 新しいカタログが入ったら、以前の検証結果は破棄して整合性を保つ。
        self._catalog = None
        self._catalog_stations = catalog.stations
        self._station_row_pairs = []
        self._checked_station_pairs = set()
        self._station_selection_dirty = False
        self._reset_execution_state_for_new_scan()
        self._station_metric_labels = dict(catalog.station_metric_labels)
        self._render_station_check_list()
        # スキャン直後は軽量情報のみ確定し、候補日探索は明示操作まで遅延させる。
        self._recompute_base_date_candidates_for_selected_stations()
        self.precheck_summary.set(
            f"軽量スキャン完了: stations={len(self._catalog_stations)} / invalid_files={len(catalog.invalid_files)}"
        )
        self.batch_status.set("待機中")
        self._append_log(
            f"[SCAN] lightweight done stations={len(self._catalog_stations)} invalid_files={len(catalog.invalid_files)}"
        )

    def _reset_execution_state_for_new_scan(self) -> None:
        # 元データが変わると検証済み対象も変わるので、結果表示を初期化する。
        self._precheck_ok_targets = []
        self._result_row_ids = {}
        self._preview_pending = None
        self.selected_base_dates = []
        self._base_dates = []
        self._base_date_year_to_months = {}
        self._base_date_year_month_to_days = {}
        self.base_date_year.set("")
        self.base_date_month.set("")
        self.base_date_candidate.set("")
        combo = getattr(self, "base_date_candidate_combo", None)
        if combo is not None:
            combo.configure(values=())
        year_combo = getattr(self, "base_date_year_combo", None)
        if year_combo is not None:
            year_combo.configure(values=())
        month_combo = getattr(self, "base_date_month_combo", None)
        if month_combo is not None:
            month_combo.configure(values=())
        if hasattr(self, "base_date_list"):
            self.base_date_list.delete(0, "end")
        if hasattr(self, "result_tree"):
            for item_id in self.result_tree.get_children():
                self.result_tree.delete(item_id)
        self._clear_preview_choices()

    def _clear_preview_choices(self) -> None:
        # プレビュー候補は前回の選択を残さない。
        self._preview_station_display_to_pair = {}
        self._preview_graph_display_to_key = {}
        self.preview_station_combo.configure(values=())
        self.preview_date_combo.configure(values=())
        self.preview_target_station.set("")
        self.preview_target_date.set("")
        combo = getattr(self, "preview_graph_combo", None)
        if combo is not None:
            combo.configure(values=())
            self.preview_target_graph.set("")
        self.preview_message.set("")
        self._preview_photo = None
        self._preview_image_bytes = None
        self._preview_canvas_image_id = None
        self._preview_placeholder_id = None
        self._preview_last_fit_size = None
        self._preview_last_image_hash = None
        self._show_preview_placeholder("プレビュー未生成")
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
        run_precheck(self)

    def _source_label(self, source: str) -> str:
        """内部 source 値を画面表示用の日本語ラベルへ変換する。"""

        return self.SOURCE_LABELS.get(source, source)

    def _station_display_text(self, source: str, station_key: str, station_name: str, checked: bool) -> str:
        """観測所チェック行の表示テキストを返す。"""

        metric_labels = self._station_metric_labels.get((source, station_key), ())
        return format_station_display_text(
            source=source,
            station_key=station_key,
            station_name=station_name,
            checked=checked,
            source_label_map=self.SOURCE_LABELS,
            metric_labels=metric_labels,
        )

    def _render_station_check_list(self) -> None:
        """観測所一覧をチェック表現で再描画する。"""

        # 再描画時にスクロールが先頭へ戻らないよう、表示位置を保持する。
        yview = self.station_list.yview()
        self.station_list.delete(0, "end")
        self._station_row_pairs = []
        total = len(self._catalog_stations)
        for idx, (source, station_key, station_name) in enumerate(self._catalog_stations):
            pair = (source, station_key)
            checked = pair in self._checked_station_pairs
            self.station_list.insert("end", self._station_display_text(source, station_key, station_name, checked))
            self._station_row_pairs.append(pair)
            # Listbox は行間調整 API が薄いため、空行スペーサーを挟んで視認性を上げる。
            if idx < total - 1:
                self.station_list.insert("end", " ")
                self._station_row_pairs.append(None)
        self.station_list.selection_clear(0, "end")
        if yview:
            self.station_list.yview_moveto(float(yview[0]))

    def _update_station_row_display(self, index: int) -> None:
        """指定行の表示だけ更新する。"""

        if index < 0 or index >= len(self._station_row_pairs):
            return
        pair = self._station_row_pairs[index]
        if pair is None:
            return
        source, station_key = pair
        station_name = ""
        for s, k, name in self._catalog_stations:
            if s == source and k == station_key:
                station_name = name
                break
        checked = pair in self._checked_station_pairs
        self.station_list.delete(index)
        self.station_list.insert(index, self._station_display_text(source, station_key, station_name, checked))

    def _selected_station_pairs_in_order(self) -> list[tuple[str, str]]:
        """チェック済み観測所を表示順で返す。"""

        return [pair for pair in self._station_row_pairs if pair is not None and pair in self._checked_station_pairs]

    def _toggle_station_at_index(self, index: int) -> None:
        """指定行の観測所チェックをトグルし、候補日を再計算する。"""

        if index < 0 or index >= len(self._station_row_pairs):
            return
        pair = self._station_row_pairs[index]
        if pair is None:
            return
        if pair in self._checked_station_pairs:
            self._checked_station_pairs.remove(pair)
        else:
            self._checked_station_pairs.add(pair)
        self._station_selection_dirty = True
        # 1件トグル時は該当行だけ更新してスクロール位置を維持する。
        self._update_station_row_display(index)

    def _station_checkbox_hit_width(self) -> int:
        """チェック記号として有効に扱うクリック幅(px)を返す。"""

        try:
            font = tkfont.nametofont(str(self.station_list.cget("font")))
            # "☐ " ぶんの描画幅 + 余裕を当たり判定に使う。
            return max(18, int(font.measure("☐ ")) + 6)
        except Exception:  # noqa: BLE001
            return 24

    def _on_station_list_click(self, event) -> str:
        """観測所リストクリックでチェック状態を切り替える。"""

        try:
            index = int(self.station_list.nearest(event.y))
        except Exception:  # noqa: BLE001
            return "break"
        bbox = self.station_list.bbox(index)
        if not bbox:
            return "break"
        pair = self._station_row_pairs[index] if 0 <= index < len(self._station_row_pairs) else None
        if pair is None:
            return "break"
        x, _y, _w, _h = bbox
        if int(getattr(event, "x", 0)) > x + self._station_checkbox_hit_width():
            # ラベル領域クリックではトグルせず、チェック記号だけを当たり判定にする。
            return "break"
        self._toggle_station_at_index(index)
        return "break"

    def _select_all_stations(self) -> None:
        """観測所を全選択する。"""

        self._checked_station_pairs = {(source, key) for source, key, _name in self._catalog_stations}
        self._station_selection_dirty = True
        self._render_station_check_list()

    def _clear_all_stations(self) -> None:
        """観測所選択を全解除する。"""

        self._checked_station_pairs.clear()
        self._station_selection_dirty = True
        self._render_station_check_list()

    def _apply_station_checks(self) -> None:
        """観測所チェックを基準日候補へ反映する。"""

        selected_pairs = self._selected_station_pairs_in_order()
        if selected_pairs:
            if not self._ensure_full_catalog_loaded():
                return
        self._recompute_base_date_candidates_for_selected_stations()
        self._station_selection_dirty = False
        self._append_log(f"[STATION] apply checks selected={len(selected_pairs)} dates={len(self._base_dates)}")

    def _recompute_base_date_candidates_for_selected_stations(self) -> None:
        """選択条件に合う基準日候補を更新する。"""

        catalog = self._catalog
        if catalog is None or catalog.data.empty:
            self._base_dates = []
        else:
            selected_pairs = self._selected_station_pairs_in_order()
            if not selected_pairs:
                self._base_dates = []
            else:
                station_frame = catalog.data.loc[:, ["source", "station_key", "observed_at"]].copy()
                selected_frame = pd.DataFrame(selected_pairs, columns=["source", "station_key"])
                station_frame = station_frame.merge(selected_frame, on=["source", "station_key"], how="inner")
                observed = pd.to_datetime(station_frame["observed_at"], errors="coerce").dropna()
                self._base_dates = sorted({ts.date().isoformat() for ts in observed})
        self._refresh_base_date_ymd_controls()

    def _ensure_full_catalog_loaded(self) -> bool:
        """詳細読込が必要な処理向けにフルカタログを確保する。"""

        if self._catalog is not None and not self._catalog.data.empty:
            return True
        parquet_dir = self.parquet_dir.get().strip()
        if not parquet_dir:
            messagebox.showerror("入力エラー", "Parquet ディレクトリを指定してください。")
            return False
        try:
            self._append_log(f"[SCAN] detailed load start {parquet_dir}")
            catalog = self.service.scan_catalog(parquet_dir)
        except Exception as exc:  # noqa: BLE001
            messagebox.showerror("読込エラー", str(exc))
            return False
        self._catalog = catalog
        self._append_log(
            f"[SCAN] detailed load done rows={len(catalog.data)} invalid_files={len(catalog.invalid_files)}"
        )
        return True

    def _refresh_base_date_ymd_controls(self) -> None:
        """基準日候補を YYYY/MM/DD の3プルダウンへ反映する。"""

        year_to_months: dict[str, set[str]] = {}
        year_month_to_days: dict[tuple[str, str], set[str]] = {}
        for iso_date in self._base_dates:
            if len(iso_date) < 10:
                continue
            year = iso_date[0:4]
            month = iso_date[5:7]
            day = iso_date[8:10]
            year_to_months.setdefault(year, set()).add(month)
            year_month_to_days.setdefault((year, month), set()).add(day)

        self._base_date_year_to_months = {k: sorted(v) for k, v in year_to_months.items()}
        self._base_date_year_month_to_days = {k: sorted(v) for k, v in year_month_to_days.items()}

        years = sorted(self._base_date_year_to_months.keys())
        year_combo = getattr(self, "base_date_year_combo", None)
        if year_combo is not None:
            year_combo.configure(values=years)
        selected_year = self.base_date_year.get().strip()
        if selected_year not in years:
            selected_year = years[0] if years else ""
            self.base_date_year.set(selected_year)

        months = self._base_date_year_to_months.get(selected_year, [])
        month_combo = getattr(self, "base_date_month_combo", None)
        if month_combo is not None:
            month_combo.configure(values=months)
        selected_month = self.base_date_month.get().strip()
        if selected_month not in months:
            selected_month = months[0] if months else ""
            self.base_date_month.set(selected_month)

        days = self._base_date_year_month_to_days.get((selected_year, selected_month), [])
        day_combo = getattr(self, "base_date_candidate_combo", None)
        if day_combo is not None:
            day_combo.configure(values=days)
        selected_day = self.base_date_candidate.get().strip()
        if selected_day not in days:
            selected_day = days[0] if days else ""
            self.base_date_candidate.set(selected_day)

    def _on_base_date_year_changed(self, _event=None) -> None:
        """年変更時に月/日候補を更新する。"""

        self.base_date_month.set("")
        self.base_date_candidate.set("")
        self._refresh_base_date_ymd_controls()

    def _on_base_date_month_changed(self, _event=None) -> None:
        """月変更時に日候補を更新する。"""

        self.base_date_candidate.set("")
        self._refresh_base_date_ymd_controls()

    def _current_base_date_candidate_iso(self) -> str | None:
        """現在選択の YYYY-MM-DD を返す。"""

        year = self.base_date_year.get().strip()
        month = self.base_date_month.get().strip()
        day = self.base_date_candidate.get().strip()
        if not (year and month and day):
            return None
        if (year not in self._base_date_year_to_months) or (month not in self._base_date_year_to_months.get(year, [])):
            return None
        if day not in self._base_date_year_month_to_days.get((year, month), []):
            return None
        return f"{year}-{month}-{day}"

    def _add_base_date_from_candidate(self) -> None:
        """基準日候補を追加する。"""
        add_base_date_from_candidate(self)

    def _remove_selected_base_dates(self) -> None:
        """選択中の基準日を削除する。"""
        remove_selected_base_dates(self)

    def _clear_base_dates(self) -> None:
        """基準日を全削除する。"""
        clear_base_dates(self)

    def _export_base_dates_csv(self) -> None:
        """基準日リストをCSV保存する。"""
        export_base_dates_csv(self)

    def _import_base_dates_csv(self) -> None:
        """基準日リストをCSV読込する。"""
        import_base_dates_csv(self)

    def _current_event_window_days_list(self) -> list[int]:
        """グラフ表の選択状態から窓リストを返す。"""

        matrix = getattr(self, "graph_cell_vars", {})
        if isinstance(matrix, dict) and matrix:
            days: list[int] = []
            if any(bool(matrix.get(key).get()) for key in ("hyetograph:3day", "hydrograph_discharge:3day", "hydrograph_water_level:3day") if matrix.get(key) is not None):
                days.append(3)
            if any(bool(matrix.get(key).get()) for key in ("hyetograph:5day", "hydrograph_discharge:5day", "hydrograph_water_level:5day") if matrix.get(key) is not None):
                days.append(5)
            return days

        # フォールバック（旧変数）
        days: list[int] = []
        if bool(self.event_window_3.get()):
            days.append(3)
        if bool(self.event_window_5.get()):
            days.append(5)
        return days

    def _refresh_preview_choices(self) -> None:
        """プレビューで選べる対象候補を更新する。"""
        refresh_preview_choices(self)

    def _create_style_control(
        self,
        parent: ttk.Frame,
        *,
        row: int,
        field: dict[str, Any],
    ) -> dict[str, Any]:
        """スタイルフォームの 1 項目を構築して制御情報を返す。"""

        kind = str(field.get("kind", "str"))
        label = str(field.get("label", ""))
        path = str(field.get("path", ""))
        label_widget = ttk.Label(parent, text=label)
        label_widget.grid(row=row, column=0, sticky="w", padx=6, pady=3)

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

        return {
            "path": path,
            "label": label,
            "kind": kind,
            "var": var,
            "label_widget": label_widget,
            "widget": widget,
            "tooltip": str(field.get("tooltip", "")).strip(),
        }

    def _current_style_graph_key(self) -> str:
        """現在の編集対象グラフキーを返す。"""

        key = self._current_preview_graph_key()
        if key is not None:
            return key
        return STYLE_TARGET_ORDER[0]

    def _current_preview_graph_key(self) -> str | None:
        """プレビュー出力対象で選ばれている graph key を返す。"""

        display = self.preview_target_graph.get().strip()
        key = self._preview_graph_display_to_key.get(display, display)
        if key in STYLE_GRAPH_KEYS:
            return key
        return None

    def _graph_style_fields_for(self, graph_style: dict[str, Any]) -> list[dict[str, Any]]:
        """対象グラフに応じた編集項目定義を返す。"""

        fields = [dict(field) for field in BASE_GRAPH_STYLE_FIELDS]
        if nested_value(graph_style, "x_axis.tick_interval_hours", None) is not None:
            fields.append(
                {
                    "path": "x_axis.tick_interval_hours",
                    "label": "X軸間隔(時間)",
                    "kind": "int",
                    "tooltip": "X軸の時刻目盛りを何時間間隔で表示するかを指定します。",
                }
            )
        if nested_value(graph_style, "bar_color", None) is not None:
            fields.append({"path": "bar_color", "label": "棒色", "kind": "str"})
        if nested_value(graph_style, "bar.width", None) is not None:
            fields.append({"path": "bar.width", "label": "棒幅", "kind": "float", "tooltip": "棒グラフの幅です。"})
        if nested_value(graph_style, "invert_y_axis", None) is not None:
            fields.append({"path": "invert_y_axis", "label": "Y軸反転", "kind": "bool"})
        if nested_value(graph_style, "threshold.label_enabled", None) is not None:
            fields.append(
                {
                    "path": "threshold.label_enabled",
                    "label": "基準線ラベル表示",
                    "kind": "bool",
                    "tooltip": "基準線のラベル文字をグラフ上に表示します。",
                }
            )
        if nested_value(graph_style, "threshold.label_offset", None) is not None:
            fields.append(
                {
                    "path": "threshold.label_offset",
                    "label": "基準線ラベルオフセット",
                    "kind": "float",
                    "tooltip": "基準線ラベルの上下オフセット量です。重なり回避に使います。",
                }
            )
        return fields

    def _style_label_column_minsize(self, fields: list[dict[str, Any]]) -> int:
        """スタイルフォームのラベル列最小幅を返す。"""

        try:
            font = tkfont.nametofont("TkDefaultFont")
        except Exception:  # noqa: BLE001
            return 120
        max_px = 0
        for field in fields:
            label = str(field.get("label", "")).strip()
            max_px = max(max_px, int(font.measure(label)))
        # 左右のpadding(6+6)と余白を加える。
        return max(120, max_px + 18)

    def _refresh_style_forms_from_payload(self) -> None:
        """現在のスタイル payload からフォーム表示を更新する。"""

        self._style_form_updating = True
        try:
            time_display_mode = str(nested_value(self._style_payload, "display.time_display_mode", "datetime")).strip() or "datetime"
            if time_display_mode not in VALID_TIME_DISPLAY_MODES:
                time_display_mode = "datetime"
            if self.time_display_mode.get() != time_display_mode:
                self.time_display_mode.set(time_display_mode)
            graph_key = self._current_style_graph_key()
            graph_styles = self._style_payload.setdefault("graph_styles", {})
            graph_style = graph_styles.get(graph_key)
            if not isinstance(graph_style, dict):
                graph_style = {}
                graph_styles[graph_key] = graph_style
            self.graph_style_box.configure(
                text=f"グラフ別設定 ({STYLE_TARGET_LABELS.get(graph_key, graph_key)})"
            )
            for child in self.graph_style_box.winfo_children():
                child.destroy()
            fields = self._graph_style_fields_for(graph_style)
            label_col_minsize = self._style_label_column_minsize(fields)
            self.graph_style_box.columnconfigure(0, minsize=label_col_minsize)
            self.graph_style_box.columnconfigure(1, weight=1)
            display_mode_box = getattr(self, "display_mode_box", None)
            if display_mode_box is not None:
                display_mode_box.columnconfigure(0, minsize=label_col_minsize)
            self._style_graph_controls = []
            self._style_field_tooltips = []
            for row, field in enumerate(fields):
                control = self._create_style_control(
                    self.graph_style_box,
                    row=row,
                    field=field,
                )
                value = nested_value(graph_style, control["path"], None)
                self._set_control_var(control, value)
                tip = str(control.get("tooltip", "")).strip()
                if tip:
                    label_widget = control.get("label_widget")
                    widget = control.get("widget")
                    if label_widget is not None:
                        self._style_field_tooltips.append(ToolTip(label_widget, tip))
                    if widget is not None:
                        self._style_field_tooltips.append(ToolTip(widget, tip))
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

    def _on_preview_target_selection_changed(self, _event=None) -> None:
        """観測所や基準日の変更に合わせてプレビュー候補を更新する。"""

        self._refresh_preview_choices()
        self._schedule_preview_refresh()

    def _on_event_window_days_changed(self, *_args) -> None:
        """3日/5日の切替時にプレビューを更新する。"""

        self._schedule_preview_refresh()

    def _on_time_display_mode_changed(self, *_args) -> None:
        """表示モードの切替時にプレビューを更新する。"""

        if self._style_form_updating:
            return
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

        graph_key = self._current_style_graph_key()
        graph_styles = self._style_payload.setdefault("graph_styles", {})
        graph_style = graph_styles.get(graph_key)
        if not isinstance(graph_style, dict):
            graph_style = {}
            graph_styles[graph_key] = graph_style
        for control in self._style_graph_controls:
            current_value = nested_value(graph_style, control["path"], None)
            value, error = self._coerce_control_value(control, current_value)
            if error:
                self.preview_message.set(error)
                return False
            set_nested_value(graph_style, control["path"], value)
        time_display_mode = str(self.time_display_mode.get()).strip() or "datetime"
        if time_display_mode not in VALID_TIME_DISPLAY_MODES:
            time_display_mode = "datetime"
        set_nested_value(self._style_payload, "display.time_display_mode", time_display_mode)
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
        render_preview(self, silent_json_error=silent_json_error)

    def _export_preview_sample(self) -> None:
        """現在プレビュー対象のサンプル画像を出力する。"""
        export_preview_sample(self)

    def _build_preview_style_payload(self, payload: dict[str, Any]) -> dict[str, Any]:
        """プレビュー用にスタイルを調整して返す。"""

        return deepcopy(payload)

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

    def _on_preview_area_resized(self, _event=None) -> None:
        """プレビュー領域サイズ変更時に画像を再フィットする。"""
        on_preview_area_resized(self, _event=_event)

    def _display_preview_image(self, image_bytes: bytes, *, force: bool = False) -> None:
        """プレビュー領域に収まるよう縮小して中央表示する。"""
        display_preview_image(self, image_bytes, force=force)

    def _current_preview_canvas_size(self) -> tuple[int, int] | None:
        """プレビューCanvasの有効表示サイズを返す。"""
        return current_preview_canvas_size(self)

    def _draw_preview_photo(self, photo: tk.PhotoImage) -> None:
        """Canvas中央にプレビュー画像を描画する。"""
        draw_preview_photo(self, photo)

    def _show_preview_placeholder(self, text: str) -> None:
        """画像未表示時のプレースホルダを描画する。"""
        show_preview_placeholder(self, text)

    def _load_thresholds_cached(self, threshold_file_path: str | None) -> ThresholdLoadResult | None:
        """しきい値ファイルの読込結果をキャッシュして使い回す。"""
        return load_thresholds_with_cache(threshold_file_path, cache=self._threshold_cache)

    def _start_batch_run(self) -> None:
        """バッチ実行を開始する。"""
        start_batch_run(self)

    def _request_stop(self) -> None:
        """バッチ停止を要求する。"""

        if self._stop_event is not None:
            self._stop_event.set()
            self.batch_status.set("停止要求中...")
            self._append_log("[RUN] stop requested")

    def _is_effective_default_style(self, payload: dict[str, Any]) -> bool:
        """現在スタイルが実質デフォルトかを判定する。"""

        current = load_style(payload=payload)
        baseline = load_style(payload=default_style())
        if any(msg.startswith("error:") for msg in current.warnings):
            return False
        return current.style == baseline.style

    def _confirm_default_style_before_run(self, payload: dict[str, Any]) -> bool:
        """デフォルトスタイル実行時の確認ダイアログ。"""

        if not self._is_effective_default_style(payload):
            return True
        response = messagebox.askyesnocancel(
            "スタイル確認",
            "スタイルが初期値のままです。スタイル調整しますか？\n\n"
            "はい: スタイル調整へ移動\n"
            "いいえ: そのまま実行\n"
            "キャンセル: 実行中止",
            icon="warning",
        )
        if response is None:
            return False
        if response is True:
            self.notebook.select(self.style_tab)
            self.preview_message.set("スタイル調整タブへ移動しました。")
            return False
        return True

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
                handle_event(self, event, payload)
        except queue.Empty:
            pass
        self.after(120, self._poll_events)

    def _append_log(self, message: str) -> None:
        """ログ欄に 1 行追加する。"""

        self.log_text.insert("end", message.rstrip() + "\n")
        self.log_text.see("end")

    def _on_result_row_double_click(self, event) -> str:
        """結果行ダブルクリック時に出力先フルパスを表示する。"""

        item_id = self.result_tree.identify_row(event.y)
        if not item_id:
            return "break"
        target_id = str(item_id)
        output_path = self._result_output_paths.get(target_id, "").strip()
        if output_path:
            messagebox.showinfo("出力先", output_path)
        else:
            messagebox.showinfo("出力先", "この行の出力先はまだありません。")
        return "break"

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
