from __future__ import annotations

import queue
import threading
import tkinter as tk
from datetime import datetime
from tkinter import filedialog, messagebox, ttk

from river_meta.services.rainfall import RainfallRunInput, run_rainfall_analyze, run_rainfall_collect


Event = tuple[str, object]

SOURCE_LABEL_TO_TOKEN = {
    "気象庁（JMA）": "jma",
    "水文水質データベース": "water_info",
    "気象庁 + 水文水質データベース": "both",
}

MODE_LABEL_TO_TOKEN = {
    "取得 + 集計": "analyze",
    "取得のみ": "collect",
}


class RainfallGuiApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Rainfall 統合GUI（独立版）")
        self.geometry("980x760")
        self.minsize(920, 680)
        self._event_queue: queue.Queue[Event] = queue.Queue()
        self._running = False
        self._stop_event: threading.Event | None = None
        self._build_ui()
        self.after(120, self._drain_events)

    def _build_ui(self) -> None:
        root = ttk.Frame(self, padding=10)
        root.pack(fill="both", expand=True)
        root.columnconfigure(0, weight=1)
        root.rowconfigure(5, weight=1)

        last_year = datetime.now().year - 1
        self.source_label = tk.StringVar(value="気象庁 + 水文水質データベース")
        self.mode_label = tk.StringVar(value="取得 + 集計")
        self.start_year = tk.StringVar(value=str(last_year))
        self.end_year = tk.StringVar(value=str(last_year))
        self.pref_list = tk.StringVar(value="大阪,京都,兵庫,和歌山,奈良")
        self.station_codes = tk.StringVar(value="")
        self.output_dir = tk.StringVar(value="")
        self.export_excel = tk.BooleanVar(value=True)
        self.export_chart = tk.BooleanVar(value=True)
        self.decimal_places = tk.StringVar(value="2")
        self.jma_log_output = tk.BooleanVar(value=False)
        self.jma_log_level = tk.StringVar(value="INFO")
        self.status = tk.StringVar(value="待機中")

        self.pref_field_label = tk.StringVar(value="都道府県（カンマ区切り）")
        self.code_field_label = tk.StringVar(value="観測所コード（カンマ区切り）")
        self.station_hint = tk.StringVar(value="")

        info_frame = ttk.LabelFrame(root, text="ツール説明", padding=8)
        info_frame.grid(row=0, column=0, sticky="ew")
        info_frame.columnconfigure(0, weight=1)
        ttk.Label(
            info_frame,
            justify="left",
            wraplength=900,
            text=(
                "このツールは、気象庁（JMA）と水文水質データベースの雨量データを共通形式で取得します。\n"
                "実行モード: 取得のみ / 取得 + 集計。\n"
                "取得元が「気象庁 + 水文水質データベース」の場合、片側のみ失敗したときは部分成功として扱い、"
                "警告をログに表示します。"
            ),
        ).grid(row=0, column=0, sticky="w")

        settings_frame = ttk.LabelFrame(root, text="実行設定", padding=8)
        settings_frame.grid(row=1, column=0, sticky="ew", pady=(8, 0))
        settings_frame.columnconfigure(1, weight=1)
        ttk.Label(settings_frame, text="取得元").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=4)
        self.source_combo = ttk.Combobox(
            settings_frame,
            textvariable=self.source_label,
            values=list(SOURCE_LABEL_TO_TOKEN.keys()),
            state="readonly",
            width=26,
        )
        self.source_combo.grid(row=0, column=1, sticky="w", pady=4)
        ttk.Label(settings_frame, text="実行モード").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=4)
        self.mode_combo = ttk.Combobox(
            settings_frame,
            textvariable=self.mode_label,
            values=list(MODE_LABEL_TO_TOKEN.keys()),
            state="readonly",
            width=26,
        )
        self.mode_combo.grid(row=1, column=1, sticky="w", pady=4)
        ttk.Label(settings_frame, text="対象年").grid(row=2, column=0, sticky="w", padx=(0, 8), pady=4)
        year_frame = ttk.Frame(settings_frame)
        year_frame.grid(row=2, column=1, sticky="w", pady=4)
        self.start_year_entry = ttk.Entry(year_frame, textvariable=self.start_year, width=6)
        self.start_year_entry.pack(side="left")
        ttk.Label(year_frame, text=" ～ ").pack(side="left")
        self.end_year_entry = ttk.Entry(year_frame, textvariable=self.end_year, width=6)
        self.end_year_entry.pack(side="left")
        self.jma_log_check = ttk.Checkbutton(
            settings_frame,
            text="JMAログ出力を有効化",
            variable=self.jma_log_output,
        )
        self.jma_log_check.grid(row=3, column=0, columnspan=2, sticky="w", pady=4)
        ttk.Label(settings_frame, text="JMAログレベル").grid(row=4, column=0, sticky="w", padx=(0, 8), pady=4)
        self.jma_log_level_combo = ttk.Combobox(
            settings_frame,
            textvariable=self.jma_log_level,
            values=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
            state="readonly",
            width=12,
        )
        self.jma_log_level_combo.grid(row=4, column=1, sticky="w", pady=4)

        station_frame = ttk.LabelFrame(root, text="観測所指定", padding=8)
        station_frame.grid(row=2, column=0, sticky="ew", pady=(8, 0))
        station_frame.columnconfigure(1, weight=1)
        ttk.Label(station_frame, textvariable=self.pref_field_label).grid(row=0, column=0, sticky="w", padx=(0, 8), pady=4)
        self.pref_entry = ttk.Entry(station_frame, textvariable=self.pref_list)
        self.pref_entry.grid(row=0, column=1, sticky="ew", pady=4)
        ttk.Label(station_frame, textvariable=self.code_field_label).grid(row=1, column=0, sticky="w", padx=(0, 8), pady=4)
        self.code_entry = ttk.Entry(station_frame, textvariable=self.station_codes)
        self.code_entry.grid(row=1, column=1, sticky="ew", pady=4)
        ttk.Label(station_frame, textvariable=self.station_hint, foreground="#555555").grid(
            row=2, column=0, columnspan=2, sticky="w", pady=(2, 0)
        )

        output_frame = ttk.LabelFrame(root, text="出力設定（必須）", padding=8)
        output_frame.grid(row=3, column=0, sticky="ew", pady=(8, 0))
        output_frame.columnconfigure(1, weight=1)
        ttk.Label(output_frame, text="出力先フォルダ").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=4)
        self.output_entry = ttk.Entry(output_frame, textvariable=self.output_dir)
        self.output_entry.grid(row=0, column=1, sticky="ew", pady=4)
        ttk.Button(
            output_frame,
            text="...",
            width=3,
            command=lambda: self._select_directory(self.output_dir),
        ).grid(row=0, column=2, sticky="w", padx=(8, 0))
        self.export_check = ttk.Checkbutton(output_frame, text="Excel出力（集計時）", variable=self.export_excel)
        self.export_check.grid(row=1, column=0, columnspan=2, sticky="w", pady=4)
        self.chart_check = ttk.Checkbutton(output_frame, text="降雨グラフPNG出力（集計時）", variable=self.export_chart)
        self.chart_check.grid(row=2, column=0, columnspan=2, sticky="w", pady=4)
        ttk.Label(output_frame, text="小数桁数").grid(row=3, column=0, sticky="w", padx=(0, 8), pady=4)
        self.decimal_entry = ttk.Entry(output_frame, textvariable=self.decimal_places, width=10)
        self.decimal_entry.grid(row=3, column=1, sticky="w", pady=4)

        action_frame = ttk.Frame(root)
        action_frame.grid(row=4, column=0, sticky="ew", pady=(10, 0))
        self.run_btn = ttk.Button(action_frame, text="実行", command=self._run)
        self.run_btn.grid(row=0, column=0, sticky="w")
        self.stop_btn = ttk.Button(action_frame, text="停止", command=self._stop, state="disabled")
        self.stop_btn.grid(row=0, column=1, sticky="w", padx=(8, 0))
        ttk.Label(action_frame, text="状態:").grid(row=0, column=2, sticky="w", padx=(16, 4))
        ttk.Label(action_frame, textvariable=self.status).grid(row=0, column=3, sticky="w")

        log_frame = ttk.LabelFrame(root, text="ログ", padding=8)
        log_frame.grid(row=5, column=0, sticky="nsew", pady=(10, 0))
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)
        self.log_text = tk.Text(log_frame, wrap="none", height=14)
        self.log_text.grid(row=0, column=0, sticky="nsew")
        scroll = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        scroll.grid(row=0, column=1, sticky="ns")
        self.log_text.configure(yscrollcommand=scroll.set)

        self.source_label.trace_add("write", self._on_option_change)
        self.mode_label.trace_add("write", self._on_option_change)
        self.export_excel.trace_add("write", self._on_option_change)
        self.export_chart.trace_add("write", self._on_option_change)
        self.jma_log_output.trace_add("write", self._on_option_change)
        self._update_state()

    def _on_option_change(self, *_args) -> None:
        self._update_state()

    def _update_state(self) -> None:
        source = self._source_token()
        mode = self._mode_token()
        if source == "jma":
            self.pref_field_label.set("JMA 都道府県（カンマ区切り）")
            self.code_field_label.set("JMA 観測所コード（カンマ区切り）")
            self.station_hint.set("例: 都道府県=大阪,京都 / 観測所コード=62001,61286")
        elif source == "water_info":
            self.pref_field_label.set("水文水質データベース 都道府県（カンマ区切り）")
            self.code_field_label.set("水文水質データベース 観測所コード（カンマ区切り）")
            self.station_hint.set("例: 都道府県=大阪,京都 / 観測所コード=1361160200060")
        else:
            self.pref_field_label.set("都道府県（カンマ区切り）※両取得元で共通")
            self.code_field_label.set("観測所コード（カンマ区切り）※共通入力")
            self.station_hint.set("both時のコードは自動振り分け（短い数字=JMA、長い数字=水文水質データベース）")

        jma_selected = source in {"jma", "both"}
        self.jma_log_check.configure(state="normal" if jma_selected else "disabled")
        if jma_selected and self.jma_log_output.get():
            self.jma_log_level_combo.configure(state="readonly")
        else:
            self.jma_log_level_combo.configure(state="disabled")

        if mode == "collect":
            self.export_excel.set(False)
            self.export_chart.set(False)
            self.export_check.configure(state="disabled")
            self.chart_check.configure(state="disabled")
            self.decimal_entry.configure(state="disabled")
        else:
            self.export_check.configure(state="normal")
            self.chart_check.configure(state="normal")
            self.decimal_entry.configure(state="normal" if self.export_excel.get() else "disabled")

    def _run(self) -> None:
        if self._running:
            return
        try:
            request = self._build_request()
        except ValueError as exc:
            messagebox.showerror("入力エラー", str(exc))
            return

        config, mode, export_excel, export_chart, output_dir, decimal_places = request
        self._stop_event = threading.Event()
        self._running = True
        self.run_btn.configure(state="disabled")
        self.stop_btn.configure(state="normal")
        self.status.set("実行中")
        self._append_log(f"[雨量統合] 実行開始 取得元={self.source_label.get()} モード={self.mode_label.get()}")

        def worker() -> None:
            try:
                if mode == "collect":
                    result = run_rainfall_collect(
                        config,
                        log=self._log_from_worker,
                        should_stop=self._stop_event.is_set,
                    )
                    self._event_queue.put(("done_collect", result))
                else:
                    result = run_rainfall_analyze(
                        config,
                        export_excel=export_excel,
                        export_chart=export_chart,
                        output_dir=output_dir,
                        decimal_places=decimal_places,
                        log=self._log_from_worker,
                        should_stop=self._stop_event.is_set,
                    )
                    self._event_queue.put(("done_analyze", result))
            except Exception as exc:  # noqa: BLE001
                self._event_queue.put(("error_exec", f"{type(exc).__name__}: {exc}"))
            finally:
                self._event_queue.put(("finalize", None))

        threading.Thread(target=worker, daemon=True).start()

    def _stop(self) -> None:
        if self._stop_event is None or self._stop_event.is_set():
            return
        self._stop_event.set()
        self.status.set("停止要求中")
        self._append_log("[雨量統合] 停止要求を受け付けました。")

    def _build_request(self) -> tuple[RainfallRunInput, str, bool, bool, str, int]:
        source = self._source_token()
        mode = self._mode_token()
        output_dir = self.output_dir.get().strip()
        if not output_dir:
            raise ValueError("出力先フォルダは必須です。")

        start_val = self.start_year.get().strip()
        end_val = self.end_year.get().strip()
        if not start_val or not end_val:
            raise ValueError("開始年と終了年の両方を入力してください。")
        try:
            start_y = int(start_val)
            end_y = int(end_val)
        except ValueError as exc:
            raise ValueError("対象年は整数で入力してください。") from exc

        if start_y > end_y:
            start_y, end_y = end_y, start_y
        years = list(range(start_y, end_y + 1))

        prefectures = self._parse_csv(self.pref_list.get())
        codes = self._parse_csv(self.station_codes.get())
        if not prefectures and not codes:
            raise ValueError("都道府県または観測所コードのどちらかを入力してください。")

        jma_prefectures: list[str] = []
        jma_codes: list[str] = []
        waterinfo_prefectures: list[str] = []
        waterinfo_codes: list[str] = []

        if source == "jma":
            jma_prefectures = prefectures
            jma_codes = codes
        elif source == "water_info":
            waterinfo_prefectures = prefectures
            waterinfo_codes = codes
        else:
            jma_prefectures = prefectures
            waterinfo_prefectures = prefectures
            jma_codes, waterinfo_codes = self._split_codes_for_both(codes)

        if source == "jma" and not (jma_prefectures or jma_codes):
            raise ValueError("JMAの都道府県または観測所コードを入力してください。")
        if source == "water_info" and not (waterinfo_prefectures or waterinfo_codes):
            raise ValueError("水文水質データベースの都道府県または観測所コードを入力してください。")

        try:
            decimal_places = int(self.decimal_places.get().strip())
        except ValueError as exc:
            raise ValueError("小数桁数は整数で入力してください。") from exc

        config = RainfallRunInput(
            source=source,
            years=years,
            interval="1hour",
            jma_prefectures=jma_prefectures,
            jma_station_codes=jma_codes,
            waterinfo_prefectures=waterinfo_prefectures,
            waterinfo_station_codes=waterinfo_codes,
            jma_log_level=self.jma_log_level.get().strip().upper() if source in {"jma", "both"} else None,
            jma_enable_log_output=bool(self.jma_log_output.get()) if source in {"jma", "both"} else None,
            include_raw=False,
        )
        return config, mode, bool(self.export_excel.get()), bool(self.export_chart.get()), output_dir, decimal_places

    def _split_codes_for_both(self, codes: list[str]) -> tuple[list[str], list[str]]:
        jma_codes: list[str] = []
        waterinfo_codes: list[str] = []
        for code in codes:
            token = code.strip()
            if token.isdigit() and len(token) >= 8:
                waterinfo_codes.append(token)
            else:
                jma_codes.append(token)
        return jma_codes, waterinfo_codes

    def _on_collect_done(self, dataset) -> None:
        records = len(dataset.records)
        self._append_log(f"[雨量統合] 取得件数: {records}件")
        for error in dataset.errors:
            self._append_log(f"[雨量統合][警告] {self._translate_error(error)}")
        self._finalize_with_summary(
            title="雨量統合（取得）",
            details=[f"取得件数: {records}件"],
            records=records,
            errors=dataset.errors,
        )

    def _on_analyze_done(self, result) -> None:
        records = len(result.dataset.records)
        ts_rows = len(result.timeseries_df)
        annual_rows = len(result.annual_max_df)
        self._append_log(f"[雨量統合] 取得件数: {records}件")
        self._append_log(f"[雨量統合] 時系列行数: {ts_rows}行")
        self._append_log(f"[雨量統合] 年最大行数: {annual_rows}行")
        for path in result.excel_paths:
            self._append_log(f"[雨量統合] Excel出力: {path}")
        for path in result.chart_paths:
            self._append_log(f"[雨量統合] グラフ出力: {path}")
        for error in result.dataset.errors:
            self._append_log(f"[雨量統合][警告] {self._translate_error(error)}")

        details = [
            f"取得件数: {records}件",
            f"時系列行数: {ts_rows}行",
            f"年最大行数: {annual_rows}行",
        ]
        if result.excel_paths:
            details.extend([f"Excel: {path}" for path in result.excel_paths])
        if result.chart_paths:
            details.append(f"グラフ: {len(result.chart_paths)}枚出力")
        self._finalize_with_summary(
            title="雨量統合（集計）",
            details=details,
            records=records,
            errors=result.dataset.errors,
        )

    def _finalize_with_summary(self, *, title: str, details: list[str], records: int, errors: list[str]) -> None:
        if errors and records > 0:
            self.status.set("部分成功")
            return
        if records == 0:
            self.status.set("エラー")
            messagebox.showerror(title, "データが取得できませんでした。\n" + "\n".join(details))
            return
        if errors:
            self.status.set("エラー")
            translated = [self._translate_error(item) for item in errors]
            messagebox.showerror(title, "\n".join([*details, "", *translated]))
            return
        if self._stop_event is not None and self._stop_event.is_set():
            self.status.set("停止完了")
            return
        self.status.set("完了")
        messagebox.showinfo(title, "\n".join(details))

    def _drain_events(self) -> None:
        try:
            while True:
                event, payload = self._event_queue.get_nowait()
                if event == "log":
                    self._append_log(str(payload))
                elif event == "done_collect":
                    self._on_collect_done(payload)
                elif event == "done_analyze":
                    self._on_analyze_done(payload)
                elif event == "error_exec":
                    self._append_log(f"[雨量統合][ERROR] {payload}")
                    self.status.set("エラー")
                    messagebox.showerror("雨量統合", str(payload))
                elif event == "finalize":
                    self._running = False
                    self.run_btn.configure(state="normal")
                    self.stop_btn.configure(state="disabled")
        except queue.Empty:
            pass
        self.after(120, self._drain_events)

    def _source_token(self) -> str:
        return SOURCE_LABEL_TO_TOKEN.get(self.source_label.get(), "both")

    def _mode_token(self) -> str:
        return MODE_LABEL_TO_TOKEN.get(self.mode_label.get(), "analyze")

    def _log_from_worker(self, message: str) -> None:
        self._event_queue.put(("log", message))

    def _append_log(self, message: str) -> None:
        self.log_text.insert("end", message.rstrip() + "\n")
        self.log_text.see("end")

    @staticmethod
    def _parse_csv(value: str) -> list[str]:
        return [item.strip() for item in str(value).split(",") if item.strip()]

    @staticmethod
    def _translate_error(error: str) -> str:
        text = str(error)
        if text == "cancelled":
            return "停止要求により処理を中断しました。"
        if text.startswith("jma:"):
            return f"気象庁: {text[4:]}"
        if text.startswith("water_info:"):
            return f"水文水質データベース: {text[11:]}"
        return text

    @staticmethod
    def _select_directory(target: tk.StringVar) -> None:
        path = filedialog.askdirectory()
        if path:
            target.set(path)


def main() -> int:
    app = RainfallGuiApp()
    app.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
