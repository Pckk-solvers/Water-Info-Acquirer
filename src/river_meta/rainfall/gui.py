"""Rainfall GUI — タブ方式 (ttk.Notebook)。

「データ取得」タブと「整理・出力」タブで構成。
共通フッターに出力先フォルダ・実行/停止ボタン・ログを配置。
"""
from __future__ import annotations

import queue
import threading
import tkinter as tk
from datetime import datetime
from tkinter import filedialog, messagebox, ttk

from river_meta.rainfall.parquet_store import scan_parquet_dir
from river_meta.services.rainfall import (
    RainfallGenerateInput,
    RainfallRunInput,
    run_rainfall_analyze,
    run_rainfall_collect,
    run_rainfall_generate,
)


Event = tuple[str, object]

SOURCE_LABEL_TO_TOKEN = {
    "気象庁（JMA）": "jma",
    "水文水質データベース": "water_info",
    "気象庁 + 水文水質データベース": "both",
}


# =========================================================================
# メインウィンドウ
# =========================================================================


class RainfallGuiApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Rainfall 雨量データツール")
        self.geometry("920x700")
        self.minsize(860, 640)
        self._event_queue: queue.Queue[Event] = queue.Queue()
        self._running = False
        self._stop_event: threading.Event | None = None
        self._build_ui()
        self.after(120, self._drain_events)

    # -----------------------------------------------------------------
    # UI 構築
    # -----------------------------------------------------------------

    def _build_ui(self) -> None:
        root = ttk.Frame(self, padding=10)
        root.pack(fill="both", expand=True)
        root.columnconfigure(0, weight=1)
        root.rowconfigure(1, weight=1)   # Notebook が伸縮
        root.rowconfigure(3, weight=1)   # ログも伸縮

        # --- タイトル ---
        ttk.Label(root, text="Rainfall 雨量データツール", font=("", 13, "bold")).grid(
            row=0, column=0, sticky="w", pady=(0, 6),
        )

        # --- Notebook (タブ) ---
        self.notebook = ttk.Notebook(root)
        self.notebook.grid(row=1, column=0, sticky="nsew")

        self.collect_tab = CollectTab(self.notebook)
        self.generate_tab = GenerateTab(self.notebook)
        self.notebook.add(self.collect_tab, text=" データ取得 ")
        self.notebook.add(self.generate_tab, text=" 整理・出力 ")
        self.notebook.bind("<<NotebookTabChanged>>", self._on_tab_changed)

        # --- 共通フッター: 出力先 + 実行ボタン ---
        footer = ttk.LabelFrame(root, text="共通設定", padding=8)
        footer.grid(row=2, column=0, sticky="ew", pady=(8, 0))
        footer.columnconfigure(1, weight=1)

        self.output_dir = tk.StringVar(value="")
        ttk.Label(footer, text="出力先フォルダ").grid(row=0, column=0, sticky="w", padx=(0, 8))
        self.output_entry = ttk.Entry(footer, textvariable=self.output_dir)
        self.output_entry.grid(row=0, column=1, sticky="ew")
        ttk.Button(footer, text="...", width=3, command=self._browse_output).grid(
            row=0, column=2, padx=(4, 0),
        )

        btn_frame = ttk.Frame(footer)
        btn_frame.grid(row=1, column=0, columnspan=3, sticky="ew", pady=(8, 0))
        self.run_btn = ttk.Button(btn_frame, text="実行", command=self._run)
        self.run_btn.pack(side="left")
        self.stop_btn = ttk.Button(btn_frame, text="停止", command=self._stop, state="disabled")
        self.stop_btn.pack(side="left", padx=(8, 0))
        self.status = tk.StringVar(value="待機中")
        ttk.Label(btn_frame, text="状態:").pack(side="left", padx=(24, 4))
        ttk.Label(btn_frame, textvariable=self.status).pack(side="left")

        # --- ログ ---
        log_frame = ttk.LabelFrame(root, text="ログ", padding=4)
        log_frame.grid(row=3, column=0, sticky="nsew", pady=(8, 0))
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)
        self.log_text = tk.Text(log_frame, wrap="none", height=8, font=("Consolas", 9))
        self.log_text.grid(row=0, column=0, sticky="nsew")
        scroll = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        scroll.grid(row=0, column=1, sticky="ns")
        self.log_text.configure(yscrollcommand=scroll.set)

    # -----------------------------------------------------------------
    # タブ切替
    # -----------------------------------------------------------------

    def _on_tab_changed(self, _event: tk.Event) -> None:  # type: ignore[type-arg]
        idx = self.notebook.index(self.notebook.select())
        if idx == 1:  # 整理タブ選択時 → Parquetスキャン
            output_dir = self.output_dir.get().strip()
            if output_dir:
                self.generate_tab.scan(output_dir)

    # -----------------------------------------------------------------
    # 実行
    # -----------------------------------------------------------------

    def _run(self) -> None:
        if self._running:
            return

        output_dir = self.output_dir.get().strip()
        if not output_dir:
            messagebox.showerror("入力エラー", "出力先フォルダを指定してください。")
            return

        idx = self.notebook.index(self.notebook.select())
        self._stop_event = threading.Event()
        self._running = True
        self.run_btn.configure(state="disabled")
        self.stop_btn.configure(state="normal")
        self.status.set("実行中")

        if idx == 0:
            self._run_collect(output_dir)
        else:
            self._run_generate(output_dir)

    def _run_collect(self, output_dir: str) -> None:
        try:
            config = self.collect_tab.build_run_input()
        except ValueError as exc:
            messagebox.showerror("入力エラー", str(exc))
            self._finalize_ui()
            return

        self._append_log("[データ取得] 実行開始")

        def worker() -> None:
            try:
                result = run_rainfall_analyze(
                    config,
                    export_excel=self.generate_tab.export_excel.get(),
                    export_chart=self.generate_tab.export_chart.get(),
                    output_dir=output_dir,
                    decimal_places=self.generate_tab.get_decimal_places(),
                    log=self._log_from_worker,
                    should_stop=self._stop_event.is_set,
                )
                self._event_queue.put(("done_collect", result))
            except Exception as exc:  # noqa: BLE001
                self._event_queue.put(("error_exec", f"{type(exc).__name__}: {exc}"))
            finally:
                self._event_queue.put(("finalize", None))

        threading.Thread(target=worker, daemon=True).start()

    def _run_generate(self, output_dir: str) -> None:
        self._append_log("[整理・出力] 実行開始")

        gen_config = RainfallGenerateInput(
            parquet_dir=output_dir,
            export_excel=self.generate_tab.export_excel.get(),
            export_chart=self.generate_tab.export_chart.get(),
            decimal_places=self.generate_tab.get_decimal_places(),
        )

        def worker() -> None:
            try:
                result = run_rainfall_generate(
                    gen_config,
                    log=self._log_from_worker,
                    should_stop=self._stop_event.is_set,
                )
                self._event_queue.put(("done_generate", result))
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
        self._append_log("[INFO] 停止要求を受け付けました。")

    # -----------------------------------------------------------------
    # 結果処理
    # -----------------------------------------------------------------

    def _on_collect_done(self, result) -> None:
        records = len(result.dataset.records)
        self._append_log(f"[データ取得] 完了 — {len(result.excel_paths)}件 Excel, {len(result.chart_paths)}件 グラフ")
        for path in result.excel_paths:
            self._append_log(f"  Excel: {path}")
        for path in result.chart_paths:
            self._append_log(f"  グラフ: {path}")
        for error in result.dataset.errors:
            self._append_log(f"  [WARN] {self._translate_error(error)}")

        details = []
        if result.excel_paths:
            details.append(f"Excel: {len(result.excel_paths)}件")
        if result.chart_paths:
            details.append(f"グラフ: {len(result.chart_paths)}枚")
        errors = result.dataset.errors

        if errors and not details:
            self.status.set("エラー")
            messagebox.showerror("データ取得", "\n".join(self._translate_error(e) for e in errors))
        elif errors:
            self.status.set("部分成功")
        elif self._stop_event and self._stop_event.is_set():
            self.status.set("停止完了")
        else:
            self.status.set("完了")
            messagebox.showinfo("データ取得", "\n".join(details) if details else "完了しました。")

    def _on_generate_done(self, result) -> None:
        total = len(result.entries)
        complete = total - len(result.incomplete_entries)
        self._append_log(f"[整理・出力] 完了 — 全{total}エントリ / 完全{complete} / 不完全{len(result.incomplete_entries)}")
        for path in result.excel_paths:
            self._append_log(f"  Excel: {path}")
        for path in result.chart_paths:
            self._append_log(f"  グラフ: {path}")
        for error in result.errors:
            self._append_log(f"  [WARN] {error}")

        details = [
            f"Parquetエントリ: {total}件（完全: {complete}）",
        ]
        if result.excel_paths:
            details.append(f"Excel: {len(result.excel_paths)}件")
        if result.chart_paths:
            details.append(f"グラフ: {len(result.chart_paths)}枚")
        if result.incomplete_entries:
            details.append(f"不完全年（スキップ）: {len(result.incomplete_entries)}件")

        if result.errors and not result.excel_paths and not result.chart_paths:
            self.status.set("エラー")
            messagebox.showerror("整理・出力", "\n".join(result.errors))
        elif self._stop_event and self._stop_event.is_set():
            self.status.set("停止完了")
        else:
            self.status.set("完了")
            messagebox.showinfo("整理・出力", "\n".join(details))

        # テーブル更新
        output_dir = self.output_dir.get().strip()
        if output_dir:
            self.generate_tab.scan(output_dir)

    # -----------------------------------------------------------------
    # イベントループ
    # -----------------------------------------------------------------

    def _drain_events(self) -> None:
        try:
            while True:
                event, payload = self._event_queue.get_nowait()
                if event == "log":
                    self._append_log(str(payload))
                elif event == "done_collect":
                    self._on_collect_done(payload)
                elif event == "done_generate":
                    self._on_generate_done(payload)
                elif event == "error_exec":
                    self._append_log(f"[ERROR] {payload}")
                    self.status.set("エラー")
                    messagebox.showerror("実行エラー", str(payload))
                elif event == "finalize":
                    self._finalize_ui()
        except queue.Empty:
            pass
        self.after(120, self._drain_events)

    def _finalize_ui(self) -> None:
        self._running = False
        self.run_btn.configure(state="normal")
        self.stop_btn.configure(state="disabled")

    # -----------------------------------------------------------------
    # ヘルパー
    # -----------------------------------------------------------------

    def _browse_output(self) -> None:
        path = filedialog.askdirectory()
        if path:
            self.output_dir.set(path)

    def _log_from_worker(self, message: str) -> None:
        self._event_queue.put(("log", message))

    def _append_log(self, message: str) -> None:
        self.log_text.insert("end", message.rstrip() + "\n")
        self.log_text.see("end")

    @staticmethod
    def _translate_error(error: str) -> str:
        text = str(error)
        if text == "cancelled":
            return "停止要求により処理を中断しました。"
        if text.startswith("jma:"):
            return f"気象庁: {text[4:]}"
        if text.startswith("water_info:"):
            return f"水文水質DB: {text[11:]}"
        return text


# =========================================================================
# タブ1: データ取得
# =========================================================================


class CollectTab(ttk.Frame):
    """取得元・対象年・観測所指定・JMAオプション。"""

    def __init__(self, parent: ttk.Notebook) -> None:
        super().__init__(parent, padding=10)
        self.columnconfigure(1, weight=1)
        self._build()

    def _build(self) -> None:
        last_year = datetime.now().year - 1

        # --- 取得元 ---
        src_frame = ttk.LabelFrame(self, text="取得元", padding=8)
        src_frame.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 6))
        self.source = tk.StringVar(value="both")
        for i, (label, token) in enumerate([
            ("気象庁（JMA）", "jma"),
            ("水文水質データベース", "water_info"),
            ("両方", "both"),
        ]):
            ttk.Radiobutton(src_frame, text=label, variable=self.source, value=token).grid(
                row=0, column=i, sticky="w", padx=(0, 16),
            )

        # --- 対象年 ---
        year_frame = ttk.LabelFrame(self, text="対象年", padding=8)
        year_frame.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(0, 6))
        self.start_year = tk.StringVar(value=str(last_year))
        self.end_year = tk.StringVar(value=str(last_year))
        ttk.Entry(year_frame, textvariable=self.start_year, width=8).pack(side="left")
        ttk.Label(year_frame, text=" ～ ").pack(side="left")
        ttk.Entry(year_frame, textvariable=self.end_year, width=8).pack(side="left")

        # --- 観測所指定 ---
        station_frame = ttk.LabelFrame(self, text="観測所指定", padding=8)
        station_frame.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(0, 6))
        station_frame.columnconfigure(1, weight=1)
        ttk.Label(station_frame, text="都道府県（カンマ区切り）").grid(row=0, column=0, sticky="w", padx=(0, 8), pady=4)
        self.pref_list = tk.StringVar(value="大阪,京都,兵庫,和歌山,奈良")
        ttk.Entry(station_frame, textvariable=self.pref_list).grid(row=0, column=1, sticky="ew", pady=4)
        ttk.Label(station_frame, text="観測所コード（カンマ区切り）").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=4)
        self.station_codes = tk.StringVar(value="")
        ttk.Entry(station_frame, textvariable=self.station_codes).grid(row=1, column=1, sticky="ew", pady=4)

        # --- JMAオプション ---
        opt_frame = ttk.LabelFrame(self, text="JMAオプション", padding=8)
        opt_frame.grid(row=3, column=0, columnspan=2, sticky="ew", pady=(0, 6))
        self.jma_log_output = tk.BooleanVar(value=False)
        ttk.Checkbutton(opt_frame, text="JMAログ出力を有効化", variable=self.jma_log_output).pack(side="left")
        self.jma_log_level = tk.StringVar(value="INFO")
        ttk.Label(opt_frame, text="  レベル:").pack(side="left", padx=(16, 4))
        ttk.Combobox(
            opt_frame, textvariable=self.jma_log_level,
            values=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
            state="readonly", width=10,
        ).pack(side="left")

    # -----------------------------------------------------------------

    def build_run_input(self) -> RainfallRunInput:
        """現在のUI状態から RainfallRunInput を構築する。"""
        source = self.source.get()

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

        jma_prefs: list[str] = []
        jma_codes: list[str] = []
        wi_prefs: list[str] = []
        wi_codes: list[str] = []

        if source == "jma":
            jma_prefs, jma_codes = prefectures, codes
        elif source == "water_info":
            wi_prefs, wi_codes = prefectures, codes
        else:
            jma_prefs = prefectures
            wi_prefs = prefectures
            jma_codes, wi_codes = self._split_codes(codes)

        return RainfallRunInput(
            source=source,
            years=years,
            interval="1hour",
            jma_prefectures=jma_prefs,
            jma_station_codes=jma_codes,
            waterinfo_prefectures=wi_prefs,
            waterinfo_station_codes=wi_codes,
            jma_log_level=self.jma_log_level.get().strip().upper() if source in {"jma", "both"} else None,
            jma_enable_log_output=bool(self.jma_log_output.get()) if source in {"jma", "both"} else None,
            include_raw=False,
        )

    @staticmethod
    def _parse_csv(value: str) -> list[str]:
        return [item.strip() for item in str(value).split(",") if item.strip()]

    @staticmethod
    def _split_codes(codes: list[str]) -> tuple[list[str], list[str]]:
        jma: list[str] = []
        wi: list[str] = []
        for code in codes:
            token = code.strip()
            if token.isdigit() and len(token) >= 8:
                wi.append(token)
            else:
                jma.append(token)
        return jma, wi


# =========================================================================
# タブ2: 整理・出力
# =========================================================================


class GenerateTab(ttk.Frame):
    """Parquetスキャン結果 + 出力オプション。"""

    def __init__(self, parent: ttk.Notebook) -> None:
        super().__init__(parent, padding=10)
        self.columnconfigure(0, weight=1)
        self.rowconfigure(2, weight=1)  # Treeview が伸縮
        self._build()

    def _build(self) -> None:
        # --- ヒント ---
        hint_frame = ttk.Frame(self)
        hint_frame.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        ttk.Label(
            hint_frame,
            text="ℹ 共通設定の「出力先フォルダ」に parquet/ サブディレクトリが必要です。"
                 "データ取得後に同じフォルダを指定してください。",
            foreground="#555",
            wraplength=800,
        ).pack(side="left", fill="x")
        self.parquet_detect = tk.StringVar(value="")
        self.parquet_detect_label = ttk.Label(hint_frame, textvariable=self.parquet_detect)
        self.parquet_detect_label.pack(side="right", padx=(12, 0))

        # --- 出力オプション ---
        opt_frame = ttk.LabelFrame(self, text="出力オプション", padding=8)
        opt_frame.grid(row=1, column=0, sticky="ew", pady=(0, 6))
        self.export_excel = tk.BooleanVar(value=True)
        self.export_chart = tk.BooleanVar(value=True)
        self.decimal_places = tk.StringVar(value="2")
        ttk.Checkbutton(opt_frame, text="Excel出力", variable=self.export_excel).pack(side="left", padx=(0, 16))
        ttk.Checkbutton(opt_frame, text="降雨グラフPNG出力", variable=self.export_chart).pack(side="left", padx=(0, 16))
        ttk.Label(opt_frame, text="小数桁数:").pack(side="left", padx=(16, 4))
        ttk.Entry(opt_frame, textvariable=self.decimal_places, width=5).pack(side="left")

        # --- Parquetテーブル ---
        table_frame = ttk.LabelFrame(self, text="Parquetデータ状況", padding=4)
        table_frame.grid(row=2, column=0, sticky="nsew", pady=(0, 6))
        table_frame.columnconfigure(0, weight=1)
        table_frame.rowconfigure(0, weight=1)

        columns = ("source", "station_key", "year", "months", "complete")
        self.tree = ttk.Treeview(table_frame, columns=columns, show="headings", height=8)
        self.tree.heading("source", text="データ元")
        self.tree.heading("station_key", text="観測所キー")
        self.tree.heading("year", text="年")
        self.tree.heading("months", text="月数")
        self.tree.heading("complete", text="完全性")
        self.tree.column("source", width=100, anchor="center")
        self.tree.column("station_key", width=140, anchor="center")
        self.tree.column("year", width=60, anchor="center")
        self.tree.column("months", width=60, anchor="center")
        self.tree.column("complete", width=80, anchor="center")
        self.tree.grid(row=0, column=0, sticky="nsew")
        tree_scroll = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        tree_scroll.grid(row=0, column=1, sticky="ns")
        self.tree.configure(yscrollcommand=tree_scroll.set)

        # --- スキャンボタン ---
        btn_frame = ttk.Frame(self)
        btn_frame.grid(row=3, column=0, sticky="w")
        ttk.Button(btn_frame, text="スキャン更新", command=self._manual_scan).pack(side="left")
        self.scan_status = tk.StringVar(value="")
        ttk.Label(btn_frame, textvariable=self.scan_status, foreground="#666").pack(side="left", padx=(12, 0))

    # -----------------------------------------------------------------

    def scan(self, output_dir: str) -> None:
        """Parquetディレクトリをスキャンしてテーブルを更新する。"""
        from pathlib import Path
        parquet_dir = Path(output_dir) / "parquet"
        if parquet_dir.exists():
            file_count = len(list(parquet_dir.glob("*.parquet")))
            self.parquet_detect.set(f"✓ parquet/ 検出（{file_count}ファイル）")
            self.parquet_detect_label.configure(foreground="#228B22")
        else:
            self.parquet_detect.set("✗ parquet/ が見つかりません")
            self.parquet_detect_label.configure(foreground="#CC0000")
        entries = scan_parquet_dir(output_dir)
        self._refresh_table(entries)

    def _manual_scan(self) -> None:
        app = self.winfo_toplevel()
        output_dir = ""
        if hasattr(app, "output_dir"):
            output_dir = app.output_dir.get().strip()  # type: ignore[union-attr]
        if not output_dir:
            messagebox.showinfo("スキャン", "出力先フォルダを先に指定してください。")
            return
        self.scan(output_dir)

    def _refresh_table(self, entries) -> None:
        for item in self.tree.get_children():
            self.tree.delete(item)

        complete_count = 0
        for entry in entries:
            source_label = "JMA" if entry.source == "jma" else "水文水質DB"
            month_count = len(entry.months) if entry.months else ("—" if entry.source == "water_info" else "0")
            status = "✓" if entry.complete else "✗"
            if entry.complete:
                complete_count += 1
            self.tree.insert("", "end", values=(
                source_label, entry.station_key, entry.year, month_count, status,
            ))

        self.scan_status.set(f"{len(entries)}件（完全: {complete_count}）")

    def get_decimal_places(self) -> int:
        try:
            return int(self.decimal_places.get().strip())
        except ValueError:
            return 2


# =========================================================================
# エントリポイント
# =========================================================================


def main() -> int:
    app = RainfallGuiApp()
    app.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
