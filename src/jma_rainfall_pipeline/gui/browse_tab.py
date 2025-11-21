# gui/browse_tab.py
import logging
import platform
import subprocess
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from datetime import datetime, timedelta, time
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from jma_rainfall_pipeline.fetcher.jma_codes_fetcher import (
    fetch_prefecture_codes,
    fetch_station_codes,
)
from jma_rainfall_pipeline.controller.weather_data_controller import WeatherDataController
from jma_rainfall_pipeline.logger.app_logger import get_logger
from jma_rainfall_pipeline.utils.config_loader import (
    config_file_exists,
    get_output_directories,
)
from .error_dialog import show_error

logger = get_logger(__name__)


class BrowseWindow(ttk.Frame):
    """GUI上で期間・地点を指定し降水量データを取得するウィンドウ"""

    def __init__(self, parent):
        super().__init__(parent)
        # 都道府県・観測所情報
        self.prefs: List[Tuple[str, str]] = []
        self.code_to_pref: Dict[str, str] = {}
        self.station_method_map: Dict[str, str] = {}

        # 選択済み観測所 (pref_code, block_no, obs_method)
        self.selected_stations: List[Tuple[str, str, str]] = []
        self.selected_station_keys: Set[Tuple[str, str, str]] = set()
        self.tree_item_to_station: Dict[str, Tuple[str, str, str]] = {}

        # UIバインド変数
        self.start_date_var = tk.StringVar()
        self.end_date_var = tk.StringVar()
        self.interval_var = tk.StringVar()
        self.csv_output_var = tk.BooleanVar(value=False)  # CSV出力フラグ（初期状態はFalse）
        self.excel_output_var = tk.BooleanVar(value=True)  # Excel出力フラグ（初期状態はTrue）
        self.status_var = tk.StringVar(value="準備完了")
        self.custom_output_dir_var = tk.StringVar(value="")
        self.output_dir_display_var = tk.StringVar(value="未指定 (設定ファイルの出力先を使用)")

        # UI構築
        self._build_ui()
        self._update_fetch_button_state()

        # データ読み込み
        self._load_prefectures()

    def _build_ui(self) -> None:
        self.columnconfigure(0, weight=1)

        description = (
            "① 期間と間隔を指定し、② 都道府県と観測所を選択、③ リストに追加した観測所のデータを取得します。\n"
            "日付は YYYY-MM または YYYY-MM-DD 形式で入力できます。\n"
        )
        ttk.Label(
            self,
            text=description,
            justify=tk.LEFT,
            wraplength=760,
        ).pack(fill=tk.X, padx=10, pady=(10, 5))

        # 期間・間隔
        date_frame = ttk.LabelFrame(self, text="① 期間と間隔の指定")
        date_frame.pack(fill=tk.X, padx=10, pady=(0, 10))
        for i in range(4):
            date_frame.columnconfigure(i, weight=1)

        ttk.Label(date_frame, text="開始日").grid(row=0, column=0, sticky=tk.W, padx=5, pady=5)
        ttk.Entry(date_frame, textvariable=self.start_date_var, width=15).grid(
            row=0,
            column=1,
            sticky=tk.W,
            padx=(0, 10),
            pady=5,
        )
        ttk.Label(date_frame, text="終了日").grid(row=0, column=2, sticky=tk.W, padx=5, pady=5)
        ttk.Entry(date_frame, textvariable=self.end_date_var, width=15).grid(
            row=0,
            column=3,
            sticky=tk.W,
            padx=(0, 10),
            pady=5,
        )

        ttk.Label(date_frame, text="間隔").grid(row=1, column=0, sticky=tk.W, padx=5, pady=5)
        interval_options = ["daily", "hourly", "10min"]
        self.interval_var.set(interval_options[0])
        ttk.Combobox(
            date_frame,
            textvariable=self.interval_var,
            values=interval_options,
            width=10,
            state="readonly",
        ).grid(row=1, column=1, sticky=tk.W, padx=(0, 10), pady=5)

        quick_frame = ttk.Frame(date_frame)
        quick_frame.grid(row=1, column=2, columnspan=2, sticky=tk.W, padx=5, pady=5)
        ttk.Label(quick_frame, text="クイック選択:").pack(side=tk.LEFT)
        ttk.Button(quick_frame, text="今月", command=lambda: self._set_quick_range("this_month")).pack(side=tk.LEFT, padx=(5, 0))
        ttk.Button(quick_frame, text="先月", command=lambda: self._set_quick_range("last_month")).pack(side=tk.LEFT, padx=(5, 0))
        ttk.Button(quick_frame, text="直近7日", command=lambda: self._set_quick_range("last_7")).pack(side=tk.LEFT, padx=(5, 0))

        # 出力オプション
        output_frame = ttk.LabelFrame(self, text="出力オプション")
        output_frame.pack(fill=tk.X, padx=10, pady=(0, 10))

        checkbox_frame = ttk.Frame(output_frame)
        checkbox_frame.pack(fill=tk.X)

        ttk.Checkbutton(
            checkbox_frame,
            text="CSVを出力",
            variable=self.csv_output_var
        ).pack(side=tk.LEFT, padx=5, pady=5)

        ttk.Checkbutton(
            checkbox_frame,
            text="Excelを出力",
            variable=self.excel_output_var
        ).pack(side=tk.LEFT, padx=5, pady=5)

        path_frame = ttk.Frame(output_frame)
        path_frame.pack(fill=tk.X, padx=5, pady=(0, 5))
        ttk.Button(path_frame, text="出力フォルダを選択", command=self._choose_output_directory).pack(side=tk.LEFT)
        ttk.Button(path_frame, text="リセット", command=self._reset_output_directory).pack(side=tk.LEFT, padx=(5, 0))
        ttk.Label(path_frame, textvariable=self.output_dir_display_var).pack(side=tk.LEFT, padx=(10, 0))

        # デフォルト値
        today = datetime.today()
        current_month = today.strftime("%Y-%m")
        self.start_date_var.set(current_month)
        self.end_date_var.set(current_month)

        # 地点選択
        selection_frame = ttk.LabelFrame(self, text="② 地点の選択")
        selection_frame.pack(fill=tk.BOTH, expand=False, padx=10, pady=(0, 10))
        selection_frame.columnconfigure(0, weight=1)
        selection_frame.columnconfigure(1, weight=1)

        pref_container = ttk.Frame(selection_frame)
        pref_container.grid(row=0, column=0, sticky="nsew", padx=(5, 5), pady=5)
        pref_container.rowconfigure(1, weight=1)

        ttk.Label(pref_container, text="都道府県一覧").grid(row=0, column=0, sticky=tk.W)
        pref_scroll = ttk.Scrollbar(pref_container, orient=tk.VERTICAL)
        self.pref_listbox = tk.Listbox(
            pref_container,
            selectmode=tk.SINGLE,
            exportselection=False,
            height=10,
            yscrollcommand=pref_scroll.set,
        )
        self.pref_listbox.grid(row=1, column=0, sticky="nsew")
        pref_scroll.config(command=self.pref_listbox.yview)
        pref_scroll.grid(row=1, column=1, sticky="ns")
        self.pref_listbox.bind("<<ListboxSelect>>", lambda _: self._update_stations())

        station_container = ttk.Frame(selection_frame)
        station_container.grid(row=0, column=1, sticky="nsew", padx=(5, 5), pady=5)
        station_container.rowconfigure(1, weight=1)

        ttk.Label(station_container, text="観測所一覧 (Ctrl+クリックで複数選択)").grid(row=0, column=0, sticky=tk.W)
        sta_scroll = ttk.Scrollbar(station_container, orient=tk.VERTICAL)
        self.sta_listbox = tk.Listbox(
            station_container,
            selectmode=tk.EXTENDED,
            exportselection=False,
            height=10,
            yscrollcommand=sta_scroll.set,
        )
        self.sta_listbox.grid(row=1, column=0, sticky="nsew")
        sta_scroll.config(command=self.sta_listbox.yview)
        sta_scroll.grid(row=1, column=1, sticky="ns")

        # 操作ボタン
        btn_frame = ttk.Frame(self)
        btn_frame.pack(fill=tk.X, pady=(0, 5), padx=10)
        ttk.Button(btn_frame, text="リストに追加", command=self._add_to_selected).pack(side=tk.LEFT)
        ttk.Button(btn_frame, text="選択項目削除 (複数可)", command=self._remove_selected).pack(side=tk.LEFT, padx=(5, 0))
        ttk.Button(btn_frame, text="すべてクリア", command=self._clear_selected).pack(side=tk.LEFT, padx=(5, 0))
        self.fetch_button = ttk.Button(btn_frame, text="データ取得", command=self._fetch_data)
        self.fetch_button.pack(side=tk.RIGHT)

        # 選択済み一覧
        selected_frame = ttk.LabelFrame(self, text="③ 取得対象の確認 (Ctrl+クリックで複数選択)")
        selected_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=(0, 10))
        selected_frame.rowconfigure(0, weight=1)
        selected_frame.columnconfigure(0, weight=1)

        columns = ("pref", "station", "block", "method")
        self.selected_tree = ttk.Treeview(
            selected_frame,
            columns=columns,
            show="headings",
            height=6,
            selectmode="extended"  # 複数選択を有効にする
        )
        self.selected_tree.heading("pref", text="都道府県")
        self.selected_tree.heading("station", text="観測所")
        self.selected_tree.heading("block", text="JIS都道府県コード-観測所コード")
        self.selected_tree.heading("method", text="観測方式")
        for column, width in zip(columns, (140, 200, 150, 110)):
            self.selected_tree.column(column, width=width, anchor=tk.W)

        tree_scroll = ttk.Scrollbar(selected_frame, orient=tk.VERTICAL, command=self.selected_tree.yview)
        self.selected_tree.configure(yscrollcommand=tree_scroll.set)
        self.selected_tree.grid(row=0, column=0, sticky="nsew")
        tree_scroll.grid(row=0, column=1, sticky="ns")

        self.selected_tree.bind("<Double-1>", self._on_selected_double_click)
        self.selected_tree.bind("<Delete>", lambda _: self._remove_selected())

        # ステータスバー
        status_frame = ttk.Frame(self)
        status_frame.pack(fill=tk.X, padx=10, pady=(0, 10))
        ttk.Label(status_frame, textvariable=self.status_var, anchor=tk.W).pack(fill=tk.X)

    def _choose_output_directory(self) -> None:
        directory = filedialog.askdirectory(parent=self, title="出力フォルダを選択")
        if directory:
            self.custom_output_dir_var.set(directory)
            self.output_dir_display_var.set(directory)
            self._set_status(f"出力フォルダを設定しました: {directory}")

    def _reset_output_directory(self) -> None:
        if not config_file_exists():
            messagebox.showwarning(
                "出力フォルダの維持",
                "config.yml が存在しないため、出力フォルダを未指定にはできません。",
            )
            return

        self.custom_output_dir_var.set("")
        self.output_dir_display_var.set("未指定 (デフォルトへ出力)")
        self._set_status("出力フォルダの指定を解除しました")

    def _require_output_directory_selection(self) -> None:
        messagebox.showwarning(
            "出力フォルダの指定",
            "config.yml が見つからないため、出力フォルダを指定してください。",
        )
        while not self.custom_output_dir_var.get().strip():
            directory = filedialog.askdirectory(parent=self, title="出力フォルダを選択")
            if directory:
                self.custom_output_dir_var.set(directory)
                self.output_dir_display_var.set(directory)
                self._set_status(f"出力フォルダを設定しました: {directory}")
                break
            messagebox.showwarning(
                "出力フォルダ未指定",
                "出力フォルダを指定するまで操作を続行できません。",
            )

    def _get_effective_output_paths(self) -> Dict[str, Path]:
        output_dirs = get_output_directories()
        custom_dir = self.custom_output_dir_var.get().strip()
        if not custom_dir:
            return {
                "csv_dir": Path(output_dirs["csv_dir"]),
                "excel_dir": Path(output_dirs["excel_dir"]),
                "log_file": Path(output_dirs["log_file"]),
            }

        base_dir = Path(custom_dir)
        csv_dir = base_dir / "csv"
        excel_dir = base_dir / "excel"
        log_file_name = Path(output_dirs["log_file"]).name
        log_file = base_dir / "logs" / log_file_name
        return {
            "csv_dir": csv_dir,
            "excel_dir": excel_dir,
            "log_file": log_file,
        }

    def _configure_custom_log_handler(self, log_file: Path) -> logging.Handler:
        root_logger = logging.getLogger()
        handler = logging.FileHandler(log_file, encoding="utf-8")
        handler.setLevel(logging.INFO)

        formatter = None
        for existing in root_logger.handlers:
            formatter = getattr(existing, "formatter", None)
            if formatter:
                break

        if formatter is None:
            formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        handler.setFormatter(formatter)
        root_logger.addHandler(handler)
        return handler

    def _load_prefectures(self) -> None:
        try:
            self.prefs = fetch_prefecture_codes()
            self.code_to_pref = {code: name for code, name in self.prefs}
            for _, name in self.prefs:
                self.pref_listbox.insert(tk.END, name)
            self._set_status("都道府県一覧の取得が完了しました")
        except Exception as exc:  # pragma: no cover - GUIで例外ダイアログを表示
            show_error(
                self.master,
                "都道府県の取得エラー",
                "都道府県一覧の取得中にエラーが発生しました。",
                exc,
            )

    def _update_stations(self) -> None:
        self.sta_listbox.delete(0, tk.END)
        self.station_method_map.clear()
        sel = self.pref_listbox.curselection()
        if not sel:
            self._set_status("都道府県を選択すると観測所が表示されます")
            return
        pref_code = self.prefs[sel[0]][0]
        try:
            records = fetch_station_codes(pref_code)
        except Exception as exc:  # pragma: no cover - GUIで例外ダイアログを表示
            show_error(
                self.master,
                "観測所の取得エラー",
                f"{self.code_to_pref.get(pref_code, pref_code)}の観測所一覧の取得中にエラーが発生しました。",
                exc,
            )
            return
        seen = set()
        for record in records:
            block = record["block_no"]
            name = record["station"]
            if block in seen:
                continue
            seen.add(block)
            text = f"{name} ({pref_code}-{block})"
            self.station_method_map[text] = record.get("obs_method", "s1")
            self.sta_listbox.insert(tk.END, text)
        self._set_status(f"{self.code_to_pref.get(pref_code, pref_code)}の観測所を読み込みました")

    def _add_to_selected(self) -> None:
        sel_pref = self.pref_listbox.curselection()
        if not sel_pref:
            self._set_status("先に都道府県を選択してください")
            return
        pref_code, pref_name = self.prefs[sel_pref[0]][0], self.prefs[sel_pref[0]][1]
        for idx in self.sta_listbox.curselection():
            sta_text = self.sta_listbox.get(idx)
            # 都道府県コード-観測所コードの形式から抽出
            code_part = sta_text[sta_text.find("(") + 1 : sta_text.find(")")]
            if "-" in code_part:
                pref_code_from_text, block = code_part.split("-", 1)
            else:
                # 旧形式の場合は都道府県コードを使用
                block = code_part
                pref_code_from_text = pref_code
            obs = self.station_method_map.get(sta_text, "s1")
            key = (pref_code, block, obs)
            if key in self.selected_station_keys:
                continue
            station_name = sta_text.split(" (")[0]
            method_display = self._format_method(obs)
            # 都道府県コードも表示に含める
            display_code = f"{pref_code}-{block}"
            item_id = self.selected_tree.insert(
                "",
                tk.END,
                values=(pref_name, station_name, display_code, method_display),
            )
            self.selected_stations.append(key)
            self.selected_station_keys.add(key)
            self.tree_item_to_station[item_id] = key
        self._update_fetch_button_state()
        if self.selected_stations:
            self._set_status(f"{len(self.selected_stations)}件の観測所をリストに追加しています")

    def _remove_selected(self) -> None:
        selected_items = self.selected_tree.selection()
        if not selected_items:
            self._set_status("削除する観測所を選択してください（Ctrl+クリックで複数選択可）")
            return
        
        # 削除する項目数をカウント
        removed_count = 0
        for item in selected_items:
            station_key = self.tree_item_to_station.pop(item, None)
            if station_key and station_key in self.selected_station_keys:
                self.selected_station_keys.remove(station_key)
                if station_key in self.selected_stations:
                    self.selected_stations.remove(station_key)
                removed_count += 1
            self.selected_tree.delete(item)
        
        self._update_fetch_button_state()
        if removed_count > 1:
            self._set_status(f"{removed_count}件の観測所を削除しました")
        else:
            self._set_status("選択済み観測所を更新しました")

    def _clear_selected(self) -> None:
        for item in self.selected_tree.get_children():
            self.selected_tree.delete(item)
        self.selected_stations.clear()
        self.selected_station_keys.clear()
        self.tree_item_to_station.clear()
        self._update_fetch_button_state()
        self._set_status("選択済み観測所をすべてクリアしました")

    def _parse_date_input(self, date_str: str) -> Tuple[datetime, datetime]:
        """日付入力を検証し datetime を返す"""

        from jma_rainfall_pipeline.utils.date_utils import validate_date_range

        return validate_date_range(date_str, date_str)

    def _fetch_data(self) -> None:
        logger.info("Starting data fetch from GUI")
        try:
            start_date_str = self.start_date_var.get()
            end_date_str = self.end_date_var.get()
            start_date, _ = self._parse_date_input(start_date_str)
            _, end_date = self._parse_date_input(end_date_str)
        except ValueError:
            messagebox.showerror("日付エラー", "YYYY-MM-DD形式またはYYYY-MM形式で入力してください")
            self._set_status("日付の形式に誤りがあります")
            return

        if not config_file_exists() and not self.custom_output_dir_var.get().strip():
            messagebox.showwarning(
                "出力フォルダの指定",
                "出力フォルダの指定をしてから再度データ取得してください。",
            )
            self._set_status("出力フォルダを指定してください")
            return

        start = datetime.combine(start_date, time.min)
        end = datetime.combine(end_date, time.max)

        if not self.selected_stations:
            messagebox.showwarning("選択エラー", "少なくとも1つ選択してください")
            self._set_status("観測所をリストに追加してください")
            return

        interval_option = self.interval_var.get()
        if interval_option == "daily":
            interval = timedelta(days=1)
        elif interval_option == "hourly":
            interval = timedelta(hours=1)
        else:
            interval = timedelta(minutes=10)

        valid_stations = list(self.selected_stations)
        if not valid_stations:
            messagebox.showwarning("エラー", "有効な観測所が選択されていません")
            self._set_status("観測所が選択されていません")
            return

        output_paths = self._get_effective_output_paths()
        csv_dir: Path = output_paths["csv_dir"]
        excel_dir: Path = output_paths["excel_dir"]
        log_file_path: Path = output_paths["log_file"]

        csv_dir.mkdir(parents=True, exist_ok=True)
        if self.excel_output_var.get():
            excel_dir.mkdir(parents=True, exist_ok=True)
        log_file_path.parent.mkdir(parents=True, exist_ok=True)

        use_custom_output = bool(self.custom_output_dir_var.get().strip())
        custom_log_handler: logging.Handler | None = None
        if use_custom_output:
            try:
                custom_log_handler = self._configure_custom_log_handler(log_file_path)
            except Exception as exc:
                logger.warning("Failed to attach custom log handler: %s", exc)
                custom_log_handler = None

        self._set_status("データの取得を開始しました。処理中です…")
        self.fetch_button.state(["disabled"])
        try:
            logger.info(
                "Fetching weather data for %s stations from %s to %s with %s interval",
                len(valid_stations),
                start,
                end,
                interval_option,
            )
            controller = WeatherDataController(interval=interval)
            controller.fetch_and_export_data(
                valid_stations,
                start,
                end,
                csv_dir,
                export_csv=self.csv_output_var.get(),
                export_excel=self.excel_output_var.get(),
                excel_output_dir=excel_dir,
            )
            logger.info("Weather data fetch and export completed successfully")

            current_time = datetime.now().timestamp()
            # Get files from appropriate directories based on output settings
            output_files = []
            if self.csv_output_var.get():
                output_files.extend(list(csv_dir.glob("*.csv")))
            if self.excel_output_var.get():
                output_files.extend(list(excel_dir.glob("*.xlsx")))
            
            recent_files = [
                f for f in output_files if (current_time - f.stat().st_mtime) < 60
            ]

            if recent_files:
                files_list = "\n".join([f"・{f.name}" for f in recent_files])
                output_info = []
                if self.csv_output_var.get():
                    output_info.append(f"CSV出力先: {csv_dir}")
                if self.excel_output_var.get():
                    output_info.append(f"Excel出力先: {excel_dir}")
                output_info.append(f"ログ出力先: {log_file_path}")

                message = (
                    f"以下のファイルをエクスポートしました:\n\n{files_list}\n\n" +
                    "\n".join(output_info)
                )
            else:
                output_info = []
                if self.csv_output_var.get():
                    output_info.append(f"CSV出力先: {csv_dir}")
                if self.excel_output_var.get():
                    output_info.append(f"Excel出力先: {excel_dir}")
                output_info.append(f"ログ出力先: {log_file_path}")

                if output_info:
                    message = (
                        f"データをエクスポートしました。\n" +
                        "\n".join(output_info)
                    )
                else:
                    message = "データをエクスポートしました。"

            message += "\n\n出力先フォルダを開きますか？"
            open_directory = messagebox.askyesno("完了", message)
            if open_directory:
                # 出力されるディレクトリを開く（優先順位: CSV > Excel）
                if self.csv_output_var.get():
                    self._open_output_directory(str(csv_dir))
                elif self.excel_output_var.get():
                    self._open_output_directory(str(excel_dir))
                else:
                    self._open_output_directory(str(log_file_path.parent))
            self._set_status("データの取得とエクスポートが完了しました")
        except Exception as exc:  # pragma: no cover - GUIで例外ダイアログを表示
            show_error(
                self.master,
                "データ取得エラー",
                "データの取得中にエラーが発生しました。",
                exc,
            )
            self._set_status("データ取得中にエラーが発生しました")
        finally:
            if custom_log_handler:
                root_logger = logging.getLogger()
                root_logger.removeHandler(custom_log_handler)
                custom_log_handler.close()
            self._update_fetch_button_state()

    def _set_quick_range(self, preset: str) -> None:
        today = datetime.today()
        if preset == "this_month":
            start = today.replace(day=1)
            self.start_date_var.set(start.strftime("%Y-%m"))
            self.end_date_var.set(today.strftime("%Y-%m"))
        elif preset == "last_month":
            first_this_month = today.replace(day=1)
            last_month_end = first_this_month - timedelta(days=1)
            start = last_month_end.replace(day=1)
            self.start_date_var.set(start.strftime("%Y-%m"))
            self.end_date_var.set(last_month_end.strftime("%Y-%m"))
        elif preset == "last_7":
            start = today - timedelta(days=6)
            self.start_date_var.set(start.strftime("%Y-%m-%d"))
            self.end_date_var.set(today.strftime("%Y-%m-%d"))
        self._set_status("クイック選択を適用しました")

    def _update_fetch_button_state(self) -> None:
        if self.selected_stations:
            self.fetch_button.state(["!disabled"])
        else:
            self.fetch_button.state(["disabled"])

    def _set_status(self, message: str) -> None:
        self.status_var.set(message)
        self.update_idletasks()

    @staticmethod
    def _format_method(method_code: str) -> str:
        if method_code == "s":
            return "気象台ほか (s)"
        if method_code == "a":
            return "アメダス (a)"
        return method_code

    def _open_output_directory(self, output_dir: str) -> None:
        try:
            system = platform.system()
            if system == "Windows":
                subprocess.run(["explorer", output_dir], check=False)
            elif system == "Darwin":
                subprocess.run(["open", output_dir], check=False)
            else:
                subprocess.run(["xdg-open", output_dir], check=False)
        except Exception as exc:
            logger.warning("Failed to open output directory: %s", exc)

    def _on_selected_double_click(self, event) -> None:
        item = self.selected_tree.identify_row(event.y)
        if item:
            self.selected_tree.selection_set(item)
            self._remove_selected()
