"""Rainfall GUI — タブ方式 (ttk.Notebook)。

「データ取得」タブと「整理・出力」タブで構成。
共通フッターに出力先フォルダ・実行/停止ボタン・ログを配置。
"""
from __future__ import annotations

import inspect
import json
import math
import os
import queue
import threading
import tkinter as tk
from datetime import datetime
from tkinter import filedialog, messagebox, ttk

from river_meta.rainfall.parquet_store import scan_parquet_dir
from river_meta.services import (
    RainfallGenerateInput,
    RainfallParquetPeriodBatchExportInput,
    RainfallParquetPeriodExportInput,
    RainfallParquetPeriodExportTarget,
    RainfallRunInput,
    export_period_targets_csv,
    load_period_targets_csv,
    run_rainfall_analyze,
    run_rainfall_generate,
    run_rainfall_parquet_period_batch_export,
    run_rainfall_parquet_period_export,
)
from pathlib import Path
from .gui_station_selector import StationSelector
from .tooltip import ToolTip

try:
    import psutil
except Exception:  # noqa: BLE001
    psutil = None


Event = tuple[str, object]


def _supports_generate_input_arg(arg_name: str) -> bool:
    """RainfallGenerateInput が指定引数を受け取れるかを判定する。"""
    try:
        parameters = inspect.signature(RainfallGenerateInput).parameters.values()
    except (TypeError, ValueError):
        return False
    names = {parameter.name for parameter in parameters}
    if arg_name in names:
        return True
    return any(parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters)


def _supports_run_input_arg(arg_name: str) -> bool:
    """RainfallRunInput が指定引数を受け取れるかを判定する。"""
    try:
        parameters = inspect.signature(RainfallRunInput).parameters.values()
    except (TypeError, ValueError):
        return False
    names = {parameter.name for parameter in parameters}
    if arg_name in names:
        return True
    return any(parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters)


# =========================================================================
# メインウィンドウ
# =========================================================================


class RainfallGuiApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("RainfallCollector")
        self.geometry("1560x860")
        self.minsize(1360, 740)
        self._event_queue: queue.Queue[Event] = queue.Queue()
        self._running = False
        self._stop_event: threading.Event | None = None
        self._close_requested = False
        self._tooltips: list[ToolTip] = []
        self._setup_visual_styles()
        self._build_ui()
        self.protocol("WM_DELETE_WINDOW", self._on_close)
        self._sync_run_button_label()
        self._sync_output_label()
        self.after(120, self._drain_events)
        self.after_idle(self._stabilize_initial_layout)

    def _setup_visual_styles(self) -> None:
        """枠線を減らし、見出し主体の軽量なセクションスタイルを定義する。"""
        style = ttk.Style(self)
        style.configure("Soft.TLabelframe", borderwidth=0, relief="flat", padding=6)
        style.configure("Soft.TLabelframe.Label", foreground="#334155", font=("", 9, "bold"))

    # -----------------------------------------------------------------
    # UI 構築
    # -----------------------------------------------------------------

    def _build_ui(self) -> None:
        root = ttk.Frame(self, padding=10)
        root.pack(fill="both", expand=True)
        # 左: メイン操作 / 右: ログ（ほぼ 1:1）
        root.columnconfigure(0, weight=1, minsize=620)
        root.columnconfigure(1, weight=1, minsize=620)
        root.rowconfigure(1, weight=1)   # Notebook が主に伸縮

        # --- タイトル ---
        ttk.Label(root, text="RainfallCollector", font=("", 13, "bold")).grid(
            row=0, column=0, sticky="w", pady=(0, 6),
        )

        # --- Notebook (タブ) ---
        self.notebook = ttk.Notebook(root)
        self.notebook.grid(row=1, column=0, sticky="nsew")

        self.collect_tab = CollectTab(self.notebook)
        self.generate_tab = GenerateTab(self.notebook)
        self.period_export_tab = PeriodCsvExportTab(self.notebook)
        self.notebook.add(self.collect_tab, text=" データ取得 ")
        self.notebook.add(self.generate_tab, text=" 整理・出力 ")
        self.notebook.add(self.period_export_tab, text=" 期間CSV出力 ")
        self.notebook.bind("<<NotebookTabChanged>>", self._on_tab_changed)

        # --- 共通フッター: 出力先 + 実行ボタン ---
        footer = ttk.LabelFrame(root, text="共通設定", padding=6, style="Soft.TLabelframe")
        footer.grid(row=2, column=0, sticky="ew", pady=(8, 0))
        footer.columnconfigure(1, weight=1)

        self.output_dir = tk.StringVar(value="")
        self.output_label = ttk.Label(footer, text="出力先フォルダ")
        self.output_label.grid(row=0, column=0, sticky="w", padx=(0, 8))
        self.output_entry = ttk.Entry(footer, textvariable=self.output_dir)
        self.output_entry.grid(row=0, column=1, sticky="ew")
        self.browse_btn = ttk.Button(
            footer, text="...", width=3, command=self._browse_output, style="StationColor.TButton",
        )
        self.browse_btn.grid(row=0, column=2, padx=(4, 0))

        btn_frame = ttk.Frame(footer)
        btn_frame.grid(row=1, column=0, columnspan=3, sticky="ew", pady=(8, 0))
        self.run_btn = ttk.Button(btn_frame, text="実行", command=self._run, style="StationColor.TButton")
        self.run_btn.pack(side="left")
        self.stop_btn = ttk.Button(
            btn_frame, text="停止", command=self._stop, state="disabled", style="StationColor.TButton",
        )
        self.stop_btn.pack(side="left", padx=(8, 0))
        self.status = tk.StringVar(value="待機中")
        ttk.Label(btn_frame, text="状態:").pack(side="left", padx=(24, 4))
        ttk.Label(btn_frame, textvariable=self.status).pack(side="left")

        # --- ログ ---
        log_frame = ttk.LabelFrame(root, text="ログ", padding=3, style="Soft.TLabelframe")
        log_frame.grid(row=0, column=1, rowspan=3, sticky="nsew", padx=(8, 0))
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)
        self.log_text = tk.Text(log_frame, wrap="none", height=18, font=("Consolas", 9))
        self.log_text.grid(row=0, column=0, sticky="nsew")
        scroll = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        scroll.grid(row=0, column=1, sticky="ns")
        xscroll = ttk.Scrollbar(log_frame, orient="horizontal", command=self.log_text.xview)
        xscroll.grid(row=1, column=0, sticky="ew")
        self.log_text.configure(yscrollcommand=scroll.set, xscrollcommand=xscroll.set)
        self._setup_tooltips()

    def _setup_tooltips(self) -> None:
        def run_tip_text() -> str:
            idx = self.notebook.index(self.notebook.select())
            if idx == 0:
                return "現在の設定でデータ取得を開始します。"
            if idx == 1:
                return "現在のParquetデータから整理・出力を実行します。"
            return "指定したParquetから期間CSVを出力します。"

        self._tooltips.extend(
            [
                ToolTip(self.run_btn, text_fn=run_tip_text),
                ToolTip(self.stop_btn, "実行中の処理に停止要求を送ります。"),
            ]
        )

    def _stabilize_initial_layout(self) -> None:
        """初回描画時の欠け対策として、レイアウトを1回再確定する。"""
        try:
            self.update_idletasks()
            self.collect_tab.refresh_layout()
            self.generate_tab.refresh_layout()
            self.period_export_tab.refresh_layout()
            width = self.winfo_width()
            height = self.winfo_height()
            if width > 10 and height > 10:
                # 一部環境で初回描画欠けが出るため、1pxだけサイズを揺らして再描画を促す
                self.geometry(f"{width + 1}x{height}")
                self.geometry(f"{width}x{height}")
            self.update_idletasks()
        except tk.TclError:
            pass

    # -----------------------------------------------------------------
    # タブ切替
    # -----------------------------------------------------------------

    def _on_tab_changed(self, _event: tk.Event) -> None:  # type: ignore[type-arg]
        self._sync_run_button_label()
        self._sync_output_label()

    # -----------------------------------------------------------------
    # 実行
    # -----------------------------------------------------------------

    def _run(self) -> None:
        if self._running:
            return

        idx = self.notebook.index(self.notebook.select())
        collect_config: RainfallRunInput | None = None
        period_export_batch_config: RainfallParquetPeriodBatchExportInput | None = None
        if idx == 0:
            output_dir = self.output_dir.get().strip()
            if not output_dir:
                messagebox.showerror("入力エラー", "出力先フォルダを指定してください。")
                return
            try:
                collect_config = self.collect_tab.build_run_input()
            except ValueError as exc:
                self.status.set("入力エラー")
                messagebox.showerror("入力エラー", str(exc))
                return
        elif idx == 1:
            output_dir = self.output_dir.get().strip()
            if not output_dir:
                messagebox.showerror("入力エラー", "出力先フォルダを指定してください。")
                return
            if self.generate_tab.use_selected_station_filter() and not self.generate_tab.get_target_stations():
                self.status.set("入力エラー")
                messagebox.showerror(
                    "入力エラー",
                    "「選択行の観測所のみ出力」が有効ですが、対象行が選択されていません。",
                )
                return
        else:
            try:
                period_export_batch_config = self.period_export_tab.build_batch_input()
            except ValueError as exc:
                self.status.set("入力エラー")
                messagebox.showerror("入力エラー", str(exc))
                return

        self._stop_event = threading.Event()
        self._running = True
        self._set_running_ui(True)
        if idx == 0:
            self.status.set("実行中（データ取得）")
        elif idx == 1:
            self.status.set("実行中（整理・出力）")
        else:
            self.status.set("実行中（期間CSV出力）")

        if idx == 0:
            self._run_collect(output_dir, collect_config)
        elif idx == 1:
            self._run_generate(output_dir)
        else:
            self._run_period_export(period_export_batch_config)

    def _run_collect(self, output_dir: str, config: RainfallRunInput | None) -> None:
        if config is None:
            self.status.set("入力エラー")
            self._finalize_ui()
            return
        self._append_log("[データ取得] 実行開始")

        def worker() -> None:
            try:
                result = run_rainfall_analyze(
                    config,
                    export_excel=False,
                    export_chart=False,
                    output_dir=output_dir,
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
        target_stations = self.generate_tab.get_target_stations()
        if target_stations:
            self._append_log(f"[整理・出力] 観測所フィルタ: {len(target_stations)}件")
        force_full_regenerate = self.generate_tab.get_force_full_regenerate()
        use_diff_mode = self.generate_tab.get_effective_use_diff_mode()
        parallel_enabled = self.generate_tab.get_parallel_enabled()
        parallel_workers = self.generate_tab.get_parallel_workers()
        if force_full_regenerate:
            self._append_log("[整理・出力] 全再生成を優先: 差分更新設定は無効化します。")
        if parallel_enabled:
            enabled_targets: list[str] = []
            if self.generate_tab.export_excel.get():
                enabled_targets.append("Excel")
            if self.generate_tab.export_chart.get():
                enabled_targets.append("グラフ")
            target_text = "・".join(enabled_targets) if enabled_targets else "出力"
            self._append_log(f"[整理・出力] 並列化: 有効（workers={parallel_workers} / {target_text} 共通）")

        generate_kwargs: dict[str, object] = {
            "parquet_dir": output_dir,
            "export_excel": self.generate_tab.export_excel.get(),
            "export_chart": self.generate_tab.export_chart.get(),
            "target_stations": target_stations,
        }
        if _supports_generate_input_arg("use_diff_mode"):
            generate_kwargs["use_diff_mode"] = use_diff_mode
        if _supports_generate_input_arg("force_full_regenerate"):
            generate_kwargs["force_full_regenerate"] = force_full_regenerate
        if _supports_generate_input_arg("excel_parallel_enabled"):
            generate_kwargs["excel_parallel_enabled"] = parallel_enabled
        if _supports_generate_input_arg("excel_parallel_workers"):
            generate_kwargs["excel_parallel_workers"] = parallel_workers
        if _supports_generate_input_arg("chart_parallel_enabled"):
            generate_kwargs["chart_parallel_enabled"] = parallel_enabled
        elif _supports_generate_input_arg("enable_chart_parallel"):
            generate_kwargs["enable_chart_parallel"] = parallel_enabled
        elif _supports_generate_input_arg("enable_chart_parallelization"):
            generate_kwargs["enable_chart_parallelization"] = parallel_enabled
        if _supports_generate_input_arg("chart_parallel_workers"):
            generate_kwargs["chart_parallel_workers"] = parallel_workers
        elif _supports_generate_input_arg("chart_workers"):
            generate_kwargs["chart_workers"] = parallel_workers
        elif _supports_generate_input_arg("chart_parallel_worker_count"):
            generate_kwargs["chart_parallel_worker_count"] = parallel_workers

        gen_config = RainfallGenerateInput(**generate_kwargs)

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

    def _run_period_export(
        self,
        batch_config: RainfallParquetPeriodBatchExportInput | None,
    ) -> None:
        if batch_config is None:
            self.status.set("入力エラー")
            self._finalize_ui()
            return
        self._append_log("[期間CSV出力] 実行開始")

        def worker() -> None:
            try:
                result = run_rainfall_parquet_period_batch_export(
                    batch_config,
                    log=self._log_from_worker,
                )
                self._event_queue.put(("done_period_export", result))
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
        notify_user = not self._close_requested
        errors = [str(e) for e in result.dataset.errors]
        cancelled = "cancelled" in errors
        translated_errors = [self._translate_error(e) for e in errors if e != "cancelled"]

        if cancelled:
            self._append_log("[データ取得] 停止")
            for error in translated_errors:
                self._append_log(f"  [WARN] {error}")
            self.status.set("停止完了")
            if notify_user:
                messagebox.showinfo("データ取得", "停止要求により処理を中断しました。")
            return

        self._append_log("[データ取得] 完了")
        for error in translated_errors:
            self._append_log(f"  [WARN] {error}")

        if translated_errors:
            self.status.set("エラー")
            if notify_user:
                messagebox.showerror("データ取得", "\n".join(translated_errors))
        else:
            self.status.set("完了")
            if notify_user:
                messagebox.showinfo("データ取得", "完了しました。")

    def _on_generate_done(self, result) -> None:
        notify_user = not self._close_requested
        excel_paths = [p for p in result.excel_paths if Path(p).exists()]
        chart_paths = [p for p in result.chart_paths if Path(p).exists()]
        errors = [str(e) for e in result.errors]
        cancelled = "cancelled" in errors
        visible_errors = [e for e in errors if e != "cancelled"]

        total = len(result.entries)
        complete = total - len(result.incomplete_entries)
        if cancelled:
            self._append_log(
                f"[整理・出力] 停止 — 全{total}エントリ / 完全{complete} / 不完全{len(result.incomplete_entries)}"
            )
        else:
            self._append_log(
                f"[整理・出力] 完了 — 全{total}エントリ / 完全{complete} / 不完全{len(result.incomplete_entries)}"
            )
            for path in excel_paths:
                self._append_log(f"  Excel: {path}")
            for path in chart_paths:
                self._append_log(f"  グラフ: {path}")
        for error in visible_errors:
            self._append_log(f"  [WARN] {error}")

        details = [
            f"Parquetエントリ: {total}件（完全: {complete}）",
        ]
        if excel_paths:
            details.append(f"Excel: {len(excel_paths)}件")
        if chart_paths:
            details.append(f"グラフ: {len(chart_paths)}枚")
        if result.incomplete_entries:
            details.append(f"不完全年（スキップ）: {len(result.incomplete_entries)}件")

        if cancelled:
            self.status.set("停止完了")
            if notify_user:
                output_details = []
                if excel_paths:
                    output_details.append(f"Excel: {len(excel_paths)}件")
                if chart_paths:
                    output_details.append(f"グラフ: {len(chart_paths)}枚")
                if output_details:
                    messagebox.showinfo(
                        "整理・出力",
                        "停止要求により処理を中断しました。\n中断前に一部出力済みです。\n" + "\n".join(output_details),
                    )
                else:
                    messagebox.showinfo("整理・出力", "停止要求により処理を中断しました。")
        elif visible_errors and not excel_paths and not chart_paths:
            self.status.set("エラー")
            if notify_user:
                messagebox.showerror("整理・出力", "\n".join(visible_errors))
        elif visible_errors:
            self.status.set("部分成功")
            msg_lines = details + ["", "エラー詳細:"] + visible_errors[:8]
            if len(visible_errors) > 8:
                msg_lines.append(f"... 他 {len(visible_errors) - 8} 件")
            if notify_user:
                messagebox.showwarning("整理・出力（部分成功）", "\n".join(msg_lines))
        else:
            self.status.set("完了")
            if notify_user:
                messagebox.showinfo("整理・出力", "\n".join(details))

        # テーブル更新
        output_dir = self.output_dir.get().strip()
        if output_dir and not self._close_requested:
            self.generate_tab.scan(output_dir)

    def _on_period_export_done(self, result) -> None:
        notify_user = not self._close_requested
        if hasattr(result, "csv_paths"):
            csv_paths = [str(path) for path in getattr(result, "csv_paths", []) if Path(str(path)).exists()]
            errors = [str(error) for error in getattr(result, "errors", [])]
            if csv_paths:
                self._append_log(f"[期間CSV出力] 完了 — {len(csv_paths)}ファイル")
                for path in csv_paths:
                    self._append_log(f"  CSV: {path}")
                for error in errors:
                    self._append_log(f"  [WARN] {error}")
                self.status.set("完了")
                if notify_user:
                    lines = [f"CSV: {len(csv_paths)}件"]
                    if errors:
                        lines += ["", "警告:"] + errors[:8]
                    messagebox.showinfo("期間CSV出力", "\n".join(lines))
                return
            self._append_log("[期間CSV出力] エラー")
            for error in errors:
                self._append_log(f"  [WARN] {error}")
            self.status.set("エラー")
            if notify_user:
                messagebox.showerror("期間CSV出力", "\n".join(errors or ["CSV 出力に失敗しました。"]))
            return

        csv_path = str(getattr(result, "csv_path", "") or "").strip()
        row_count = int(getattr(result, "row_count", 0) or 0)
        errors = [str(error) for error in getattr(result, "errors", [])]
        if csv_path and Path(csv_path).exists():
            self._append_log(f"[期間CSV出力] 完了 — {row_count}行")
            self._append_log(f"  CSV: {csv_path}")
            for error in errors:
                self._append_log(f"  [WARN] {error}")
            self.status.set("完了")
            if notify_user:
                lines = [f"CSV: {csv_path}", f"行数: {row_count}"]
                if errors:
                    lines += ["", "警告:"] + errors[:8]
                messagebox.showinfo("期間CSV出力", "\n".join(lines))
            return

        self._append_log("[期間CSV出力] エラー")
        for error in errors:
            self._append_log(f"  [WARN] {error}")
        self.status.set("エラー")
        if notify_user:
            messagebox.showerror("期間CSV出力", "\n".join(errors or ["CSV 出力に失敗しました。"]))

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
                elif event == "done_period_export":
                    self._on_period_export_done(payload)
                elif event == "error_exec":
                    formatted = self._format_exec_error(payload)
                    self._append_log(f"[ERROR] {formatted}")
                    self.status.set("エラー")
                    if not self._close_requested:
                        messagebox.showerror("実行エラー", formatted)
                elif event == "finalize":
                    self._finalize_ui()
        except queue.Empty:
            pass
        try:
            self.after(120, self._drain_events)
        except tk.TclError:
            # ウィンドウ破棄直後は after が失敗する
            pass

    def _finalize_ui(self) -> None:
        self._running = False
        self._set_running_ui(False)
        self._sync_run_button_label()
        if self._close_requested:
            self.destroy()

    # -----------------------------------------------------------------
    # ヘルパー
    # -----------------------------------------------------------------

    def _browse_output(self) -> None:
        path = filedialog.askdirectory()
        if path:
            self.output_dir.set(path)

    def _on_close(self) -> None:
        if not self._running:
            self.destroy()
            return
        if self._close_requested:
            force = messagebox.askyesno(
                "強制終了確認",
                "停止待機中です。強制終了しますか？\n処理途中の結果は失われる可能性があります。",
            )
            if force:
                self.destroy()
            return
        ok = messagebox.askyesno("終了確認", "実行中の処理を停止して終了しますか？")
        if not ok:
            return
        self._close_requested = True
        self.status.set("停止要求中（終了待機）")
        self._stop()

    def _sync_run_button_label(self) -> None:
        idx = self.notebook.index(self.notebook.select())
        if idx == 0:
            self.run_btn.configure(text="データ取得を実行")
        elif idx == 1:
            self.run_btn.configure(text="整理・出力を実行")
        else:
            self.run_btn.configure(text="期間CSV出力を実行")

    def _sync_output_label(self) -> None:
        idx = self.notebook.index(self.notebook.select())
        if idx == 2:
            self.output_label.configure(text="CSV出力先フォルダ")
        else:
            self.output_label.configure(text="出力先フォルダ")

    def _set_running_ui(self, running: bool) -> None:
        run_state = "disabled" if running else "normal"
        stop_state = "normal" if running else "disabled"
        entry_state = "disabled" if running else "normal"

        self.run_btn.configure(state=run_state)
        self.stop_btn.configure(state=stop_state)
        self.output_entry.configure(state=entry_state)
        self.browse_btn.configure(state=run_state)
        if running:
            self.notebook.state(["disabled"])
        else:
            self.notebook.state(["!disabled"])
        self.collect_tab.set_enabled(not running)
        self.generate_tab.set_enabled(not running)
        self.period_export_tab.set_enabled(not running)

    def _log_from_worker(self, message: str) -> None:
        self._event_queue.put(("log", message))

    def _append_log(self, message: str) -> None:
        self.log_text.insert("end", message.rstrip() + "\n")
        self.log_text.see("end")

    @staticmethod
    def _format_exec_error(error: object) -> str:
        text = str(error)
        lower = text.lower()
        hints: list[str] = []
        if "permission" in lower or "access is denied" in lower:
            hints.append("出力先フォルダへの書き込み権限を確認してください。")
        if "not found" in lower or "no such file" in lower:
            hints.append("出力先フォルダや必要ファイルの存在を確認してください。")
        if "timeout" in lower:
            hints.append("ネットワーク状況を確認し、対象期間を短くして再実行してください。")
        if not hints:
            return text
        return text + "\n\n対処のヒント:\n- " + "\n- ".join(hints)

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
        self._enabled = True
        self._source_buttons: list[ttk.Radiobutton] = []
        self._order_buttons: list[ttk.Radiobutton] = []
        self._tooltips: list[ToolTip] = []
        self._build()

    def _build(self) -> None:
        last_year = datetime.now().year - 1

        # --- 取得元 ---
        src_frame = ttk.LabelFrame(self, text="取得元", padding=6, style="Soft.TLabelframe")
        src_frame.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 4))
        self.source = tk.StringVar(value="jma")
        for i, (label, token) in enumerate([
            ("気象庁（JMA）", "jma"),
            ("水文水質データベース", "water_info"),
        ]):
            rb = ttk.Radiobutton(src_frame, text=label, variable=self.source, value=token)
            rb.grid(row=0, column=i, sticky="w", padx=(0, 16))
            self._source_buttons.append(rb)
        self.source.trace_add("write", lambda *_: self._on_source_changed())

        # --- 対象年 ---
        year_frame = ttk.LabelFrame(self, text="対象年", padding=6, style="Soft.TLabelframe")
        year_frame.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(0, 4))
        self.start_year = tk.StringVar(value=str(last_year))
        self.end_year = tk.StringVar(value=str(last_year))
        self.start_year_entry = ttk.Entry(year_frame, textvariable=self.start_year, width=8)
        self.start_year_entry.pack(side="left")
        ttk.Label(year_frame, text=" ～ ").pack(side="left")
        self.end_year_entry = ttk.Entry(year_frame, textvariable=self.end_year, width=8)
        self.end_year_entry.pack(side="left")

        # --- 取得順序 ---
        order_frame = ttk.LabelFrame(self, text="取得順序", padding=6, style="Soft.TLabelframe")
        order_frame.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(0, 4))
        self.collection_order = tk.StringVar(value="station_year")
        for i, (label, token) in enumerate([
            ("観測所ごと（既定）", "station_year"),
            ("年ごと", "year_station"),
        ]):
            rb = ttk.Radiobutton(order_frame, text=label, variable=self.collection_order, value=token)
            rb.grid(row=0, column=i, sticky="w", padx=(0, 16))
            self._order_buttons.append(rb)

        # --- 観測所指定 (新UI) ---
        station_frame = ttk.LabelFrame(self, text="観測所指定", padding=6, style="Soft.TLabelframe")
        station_frame.grid(row=3, column=0, columnspan=2, sticky="nsew", pady=(0, 4))
        self.rowconfigure(3, weight=1)  # 観測所指定フレームを広げる

        jma_json = Path(__file__).resolve().parents[1] / "resources" / "jma_station_index.json"
        wi_json = Path(__file__).resolve().parents[1] / "resources" / "waterinfo_station_index.json"

        self.station_selector = StationSelector(
            station_frame,
            jma_json_path=jma_json,
            waterinfo_json_path=wi_json,
        )
        self.station_selector.pack(fill="both", expand=True)

    # -----------------------------------------------------------------

    def _on_source_changed(self) -> None:
        if hasattr(self, "station_selector"):
            src = self.source.get()
            if src in {"jma", "water_info"}:
                self.station_selector.set_source(src)

    def set_enabled(self, enabled: bool) -> None:
        self._enabled = enabled
        state = "normal" if enabled else "disabled"
        for rb in self._source_buttons:
            rb.configure(state=state)
        for rb in self._order_buttons:
            rb.configure(state=state)
        self.start_year_entry.configure(state=state)
        self.end_year_entry.configure(state=state)
        self.station_selector.set_enabled(enabled)

    def refresh_layout(self) -> None:
        self.update_idletasks()
        self.station_selector.refresh_layout()

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
            raise ValueError("開始年は終了年以下で入力してください。")
        years = list(range(start_y, end_y + 1))

        codes = self.station_selector.get_selected_codes()
        if not codes:
            raise ValueError("リストから観測所を選択するか、コードを直接入力してください。")

        jma_prefs: list[str] = []
        jma_codes: list[str] = []
        wi_prefs: list[str] = []
        wi_codes: list[str] = []

        if source == "jma":
            jma_codes = codes
        elif source == "water_info":
            wi_codes = codes

        run_input_kwargs: dict[str, object] = {
            "source": source,
            "years": years,
            "interval": "1hour",
            "jma_prefectures": jma_prefs,
            "jma_station_codes": jma_codes,
            "waterinfo_prefectures": wi_prefs,
            "waterinfo_station_codes": wi_codes,
            "include_raw": False,
        }
        if _supports_run_input_arg("collection_order"):
            run_input_kwargs["collection_order"] = self.collection_order.get()

        return RainfallRunInput(**run_input_kwargs)

# =========================================================================
# タブ2: 整理・出力
# =========================================================================


class GenerateTab(ttk.Frame):
    """Parquetスキャン結果 + 出力オプション。"""

    def __init__(self, parent: ttk.Notebook) -> None:
        super().__init__(parent, padding=10)
        self.columnconfigure(0, weight=1)
        self.rowconfigure(2, weight=1)  # Treeview が伸縮
        self._enabled = True
        self._parallel_workers_min = 1
        self._parallel_workers_max = 8
        self._recommended_parallel_workers = self._parallel_workers_min
        self._scan_running = False
        self._pending_scan: tuple[str, bool] | None = None
        self._entry_by_item_id: dict[str, object] = {}
        self._jma_name_by_block: dict[str, str] = {}
        self._waterinfo_name_by_id: dict[str, str] = {}
        self._load_station_name_index()
        self._tooltips: list[ToolTip] = []
        self._build()

    def _load_station_name_index(self) -> None:
        jma_json = Path(__file__).resolve().parents[1] / "resources" / "jma_station_index.json"
        if jma_json.exists():
            try:
                with jma_json.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                by_block = data.get("by_block_no", {})
                if isinstance(by_block, dict):
                    for block_no, rows in by_block.items():
                        if not isinstance(rows, list) or not rows:
                            continue
                        name = str(rows[0].get("station_name") or "").strip()
                        if name:
                            self._jma_name_by_block[str(block_no)] = name
            except Exception:
                pass

        wi_json = Path(__file__).resolve().parents[1] / "resources" / "waterinfo_station_index.json"
        if wi_json.exists():
            try:
                with wi_json.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                by_station_id = data.get("by_station_id", {})
                if isinstance(by_station_id, dict):
                    for sid, row in by_station_id.items():
                        if not isinstance(row, dict):
                            continue
                        name = str(row.get("station_name") or "").strip()
                        if name:
                            self._waterinfo_name_by_id[str(sid)] = name
            except Exception:
                pass

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
        opt_frame = ttk.LabelFrame(self, text="出力オプション", padding=6, style="Soft.TLabelframe")
        opt_frame.grid(row=1, column=0, sticky="ew", pady=(0, 4))
        opt_frame.columnconfigure(1, weight=1)

        ttk.Label(opt_frame, text="出力対象").grid(row=0, column=0, sticky="w", padx=(0, 12))
        target_frame = ttk.Frame(opt_frame)
        target_frame.grid(row=0, column=1, sticky="w")
        self.export_excel = tk.BooleanVar(value=True)
        self.export_chart = tk.BooleanVar(value=True)
        self.export_excel_check = ttk.Checkbutton(target_frame, text="Excel出力", variable=self.export_excel)
        self.export_excel_check.pack(side="left", padx=(0, 16))
        self.export_chart_check = ttk.Checkbutton(target_frame, text="降雨グラフPNG出力", variable=self.export_chart)
        self.export_chart_check.pack(side="left", padx=(0, 16))

        ttk.Label(opt_frame, text="更新方式").grid(row=1, column=0, sticky="w", padx=(0, 12), pady=(6, 0))
        mode_frame = ttk.Frame(opt_frame)
        mode_frame.grid(row=1, column=1, sticky="w", pady=(6, 0))
        self.regenerate_mode = tk.StringVar(value="diff")
        self.regenerate_mode_diff_radio = ttk.Radiobutton(
            mode_frame,
            text="差分更新（既定）",
            variable=self.regenerate_mode,
            value="diff",
        )
        self.regenerate_mode_diff_radio.pack(side="left", padx=(0, 16))
        self.regenerate_mode_full_radio = ttk.Radiobutton(
            mode_frame,
            text="全再生成",
            variable=self.regenerate_mode,
            value="full",
        )
        self.regenerate_mode_full_radio.pack(side="left")
        ttk.Label(
            opt_frame,
            text="※ 全再生成は差分判定を行わず、対象を再作成します。",
            foreground="#666",
        ).grid(row=2, column=1, sticky="w", pady=(4, 0))
        self._sync_regenerate_option_state()

        ttk.Label(opt_frame, text="並列化").grid(row=3, column=0, sticky="w", padx=(0, 12), pady=(6, 0))
        parallel_frame = ttk.Frame(opt_frame)
        parallel_frame.grid(row=3, column=1, sticky="w", pady=(6, 0))
        self.enable_parallel = tk.BooleanVar(value=False)
        self.enable_parallel_check = ttk.Checkbutton(
            parallel_frame,
            text="有効化",
            variable=self.enable_parallel,
        )
        self.enable_parallel_check.pack(side="left")
        ttk.Label(parallel_frame, text="ワーカー数").pack(side="left", padx=(16, 4))
        self.parallel_workers = tk.IntVar(value=self._parallel_workers_min)
        self.parallel_workers_spin = ttk.Spinbox(
            parallel_frame,
            from_=self._parallel_workers_min,
            to=self._parallel_workers_max,
            width=4,
            textvariable=self.parallel_workers,
        )
        self.parallel_workers_spin.pack(side="left")
        self.recalc_parallel_workers_btn = ttk.Button(
            parallel_frame,
            text="推奨を再計算",
            command=self._recalc_parallel_workers_recommendation,
            style="StationColor.TButton",
        )
        self.recalc_parallel_workers_btn.pack(side="left", padx=(12, 0))
        self.apply_parallel_workers_btn = ttk.Button(
            parallel_frame,
            text="推奨を適用",
            command=self._apply_recommended_parallel_workers,
            style="StationColor.TButton",
        )
        self.apply_parallel_workers_btn.pack(side="left", padx=(8, 0))
        self.parallel_recommended = tk.StringVar(value="")
        ttk.Label(opt_frame, textvariable=self.parallel_recommended, foreground="#666").grid(
            row=4,
            column=1,
            sticky="w",
            pady=(4, 0),
        )
        self._recalc_parallel_workers_recommendation()

        # --- Parquetテーブル ---
        table_frame = ttk.LabelFrame(self, text="Parquetデータ状況", padding=3, style="Soft.TLabelframe")
        table_frame.grid(row=2, column=0, sticky="nsew", pady=(0, 4))
        table_frame.columnconfigure(0, weight=1)
        table_frame.rowconfigure(0, weight=1)

        columns = ("source", "station_code", "station_name", "year", "months", "complete")
        self.tree = ttk.Treeview(
            table_frame, columns=columns, show="headings", height=8, selectmode="extended",
        )
        self.tree.heading("source", text="データ元")
        self.tree.heading("station_code", text="観測所コード")
        self.tree.heading("station_name", text="観測所")
        self.tree.heading("year", text="年")
        self.tree.heading("months", text="月数")
        self.tree.heading("complete", text="完全性")
        self.tree.column("source", width=100, anchor="center")
        self.tree.column("station_code", width=140, anchor="center")
        self.tree.column("station_name", width=200, anchor="w")
        self.tree.column("year", width=60, anchor="center")
        self.tree.column("months", width=60, anchor="center")
        self.tree.column("complete", width=80, anchor="center")
        self.tree.grid(row=0, column=0, sticky="nsew")
        tree_scroll = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        tree_scroll.grid(row=0, column=1, sticky="ns")
        self.tree.configure(yscrollcommand=tree_scroll.set)
        self.tree.bind("<<TreeviewSelect>>", self._on_tree_selection_changed)

        # --- スキャンボタン ---
        btn_frame = ttk.Frame(self)
        btn_frame.grid(row=3, column=0, sticky="ew")
        self.scan_btn = ttk.Button(
            btn_frame, text="スキャン更新", command=self._manual_scan, style="StationColor.TButton",
        )
        self.scan_btn.pack(side="left")
        self.clear_selection_btn = ttk.Button(
            btn_frame, text="選択解除", command=self._clear_row_selection, style="StationColor.TButton",
        )
        self.clear_selection_btn.pack(side="left", padx=(8, 0))
        self.use_selection_filter = tk.BooleanVar(value=False)
        self.selection_filter_check = ttk.Checkbutton(
            btn_frame,
            text="選択行の観測所のみ出力",
            variable=self.use_selection_filter,
            command=self._update_target_summary,
        )
        self.selection_filter_check.pack(side="left", padx=(12, 0))
        self.target_summary = tk.StringVar(
            value="対象: 全観測所（完全データのみ出力／不完全年は自動スキップ）"
        )
        ttk.Label(btn_frame, textvariable=self.target_summary, foreground="#666").pack(side="left", padx=(12, 0))
        self.scan_status = tk.StringVar(value="")
        ttk.Label(btn_frame, textvariable=self.scan_status, foreground="#666").pack(side="right")
        self._setup_tooltips()

    def _setup_tooltips(self) -> None:
        self._tooltips.extend(
            [
                ToolTip(self.scan_btn, "出力先の parquet/ を再スキャンして一覧を更新します。"),
                ToolTip(
                    self.selection_filter_check,
                    "有効時は選択行の観測所だけを対象にします（どちらの場合も不完全年は自動スキップ）。",
                ),
                ToolTip(
                    self.regenerate_mode_full_radio,
                    "更新方式を全再生成に切り替えます。",
                ),
                ToolTip(
                    self.enable_parallel_check,
                    "有効時は Excel とグラフの出力で同じワーカー数を使って並列実行します。",
                ),
                ToolTip(
                    self.recalc_parallel_workers_btn,
                    "PCのCPUコア数と利用可能RAMから推奨ワーカー数を再計算します。",
                ),
                ToolTip(
                    self.apply_parallel_workers_btn,
                    "推奨ワーカー数をワーカー数欄に反映します。",
                ),
            ]
        )

    # -----------------------------------------------------------------

    def scan(self, output_dir: str, *, user_initiated: bool = False) -> None:
        """Parquetディレクトリを非同期スキャンしてテーブルを更新する。"""
        target = output_dir.strip()
        if not target:
            return
        if self._scan_running:
            self._pending_scan = (target, user_initiated)
            self.scan_status.set("スキャン待機中...")
            return
        self._start_scan(target, user_initiated)

    def _start_scan(self, output_dir: str, user_initiated: bool) -> None:
        self._scan_running = True
        self.scan_status.set("スキャン中...")
        self.parquet_detect.set("スキャン中...")
        self.parquet_detect_label.configure(foreground="#666")
        self._apply_scan_button_state()

        def worker() -> None:
            try:
                parquet_dir = Path(output_dir) / "parquet"
                if parquet_dir.exists():
                    file_count = sum(1 for _ in parquet_dir.glob("*.parquet"))
                    detect_text = f"✓ parquet/ 検出（{file_count}ファイル）"
                    detect_color = "#228B22"
                else:
                    detect_text = "✗ parquet/ が見つかりません"
                    detect_color = "#CC0000"
                entries = scan_parquet_dir(output_dir)
                payload = ("ok", detect_text, detect_color, entries)
            except Exception as exc:  # noqa: BLE001
                payload = ("error", f"{type(exc).__name__}: {exc}")
            try:
                self.after(0, self._finish_scan, payload, user_initiated)
            except tk.TclError:
                pass

        threading.Thread(target=worker, daemon=True).start()

    def _finish_scan(self, payload: tuple, user_initiated: bool) -> None:
        self._scan_running = False
        try:
            kind = payload[0]
            if kind == "ok":
                _kind, detect_text, detect_color, entries = payload
                self.parquet_detect.set(detect_text)
                self.parquet_detect_label.configure(foreground=detect_color)
                self._refresh_table(entries)
            else:
                _kind, message = payload
                self.parquet_detect.set("✗ スキャンに失敗しました")
                self.parquet_detect_label.configure(foreground="#CC0000")
                self.scan_status.set("スキャン失敗")
                if user_initiated and self._can_show_modal():
                    messagebox.showerror("スキャンエラー", str(message))
        finally:
            next_scan = self._pending_scan
            self._pending_scan = None
            if next_scan is not None:
                self._start_scan(next_scan[0], next_scan[1])
            else:
                self._apply_scan_button_state()

    def _can_show_modal(self) -> bool:
        app = self.winfo_toplevel()
        return not bool(getattr(app, "_close_requested", False))

    def _apply_scan_button_state(self) -> None:
        if self._enabled and not self._scan_running:
            self.scan_btn.configure(state="normal")
        else:
            self.scan_btn.configure(state="disabled")

    def _manual_scan(self) -> None:
        app = self.winfo_toplevel()
        output_dir = ""
        if hasattr(app, "output_dir"):
            output_dir = app.output_dir.get().strip()  # type: ignore[union-attr]
        if not output_dir:
            messagebox.showinfo("スキャン", "出力先フォルダを先に指定してください。")
            return
        self.scan(output_dir, user_initiated=True)

    def _refresh_table(self, entries) -> None:
        selected_pairs = self._get_selected_station_pairs()
        for item in self.tree.get_children():
            self.tree.delete(item)
        self._entry_by_item_id.clear()

        complete_count = 0
        for entry in entries:
            source_label = "気象庁" if entry.source == "jma" else "水文水質DB"
            station_code = self._to_display_station_code(entry.source, entry.station_key)
            station_name = self._resolve_display_station_name(entry.source, station_code)
            month_count = len(entry.months) if entry.months else ("—" if entry.source == "water_info" else "0")
            status = "✓" if entry.complete else "✗"
            if entry.complete:
                complete_count += 1
            iid = self.tree.insert("", "end", values=(
                source_label, station_code, station_name, entry.year, month_count, status,
            ))
            self._entry_by_item_id[iid] = entry
            if (entry.source, entry.station_key) in selected_pairs:
                self.tree.selection_add(iid)

        self.scan_status.set(f"{len(entries)}件（完全: {complete_count}）")
        self._update_target_summary()

    def _on_tree_selection_changed(self, _event: tk.Event) -> None:  # type: ignore[type-arg]
        self._update_target_summary()

    def _clear_row_selection(self) -> None:
        self.tree.selection_remove(self.tree.selection())
        self._update_target_summary()

    @staticmethod
    def _to_display_station_code(source: str, station_key: str) -> str:
        raw = str(station_key)
        if source != "jma":
            return raw
        # JMA の内部キーは "都道府県2桁_block_no" 形式なので、表示は block_no のみ
        if "_" in raw:
            return raw.split("_", 1)[1]
        return raw

    def _resolve_display_station_name(self, source: str, station_code: str) -> str:
        if source == "jma":
            return self._jma_name_by_block.get(station_code, "")
        return self._waterinfo_name_by_id.get(station_code, "")

    def _get_selected_station_pairs(self) -> set[tuple[str, str]]:
        pairs: set[tuple[str, str]] = set()
        for item_id in self.tree.selection():
            entry = self._entry_by_item_id.get(item_id)
            if entry is None:
                continue
            source = getattr(entry, "source", "")
            station_key = getattr(entry, "station_key", "")
            if str(source).strip() and str(station_key).strip():
                pairs.add((str(source), str(station_key)))
        return pairs

    def _update_target_summary(self) -> None:
        if not self.use_selection_filter.get():
            self.target_summary.set("対象: 全観測所（完全データのみ出力／不完全年は自動スキップ）")
            return
        pairs = self._get_selected_station_pairs()
        if not pairs:
            self.target_summary.set("対象: 0観測所（行を選択してください。不完全年は自動スキップ）")
            return
        self.target_summary.set(
            f"対象: {len(pairs)}観測所（選択行のみ／完全データのみ出力・不完全年は自動スキップ）"
        )

    def _recalc_parallel_workers_recommendation(self) -> None:
        recommended, reason = self._calculate_recommended_parallel_workers()
        self._recommended_parallel_workers = max(
            self._parallel_workers_min,
            min(self._parallel_workers_max, int(recommended)),
        )
        self.parallel_recommended.set(reason)

    def _apply_recommended_parallel_workers(self) -> None:
        self.parallel_workers.set(self._recommended_parallel_workers)

    def _calculate_recommended_parallel_workers(self) -> tuple[int, str]:
        physical_cores: int | None = None
        available_gb: float | None = None

        if psutil is not None:
            try:
                physical_cores = psutil.cpu_count(logical=False)
            except Exception:  # noqa: BLE001
                physical_cores = None
            try:
                available_gb = float(psutil.virtual_memory().available) / (1024 ** 3)
            except Exception:  # noqa: BLE001
                available_gb = None

        if physical_cores is None:
            physical_cores = os.cpu_count() or 1
        physical_cores = max(1, int(physical_cores))
        cpu_limit = max(1, math.floor(physical_cores * 0.5))

        if available_gb is None:
            mem_limit = 1
            mem_text = "不明"
        else:
            mem_limit = max(1, math.floor(available_gb / 1.2))
            mem_text = f"{available_gb:.1f}GB"

        recommended = min(4, cpu_limit, mem_limit)
        reason = (
            f"推奨ワーカー: {recommended} "
            f"(物理CPU: {physical_cores}コア / 利用可能RAM: {mem_text} / "
            f"CPU上限:{cpu_limit} / RAM上限:{mem_limit})"
        )
        return recommended, reason

    def use_selected_station_filter(self) -> bool:
        return bool(self.use_selection_filter.get())

    def get_parallel_enabled(self) -> bool:
        return bool(self.enable_parallel.get())

    def get_parallel_workers(self) -> int:
        try:
            workers = int(self.parallel_workers.get())
        except (ValueError, tk.TclError):
            workers = self._recommended_parallel_workers
        workers = max(self._parallel_workers_min, min(self._parallel_workers_max, workers))
        self.parallel_workers.set(workers)
        return workers

    def get_excel_parallel_enabled(self) -> bool:
        return self.get_parallel_enabled()

    def get_excel_parallel_workers(self) -> int:
        return self.get_parallel_workers()

    def get_chart_parallel_enabled(self) -> bool:
        return self.get_parallel_enabled()

    def get_chart_parallel_workers(self) -> int:
        return self.get_parallel_workers()

    def get_use_diff_mode(self) -> bool:
        return self.regenerate_mode.get() == "diff"

    def get_force_full_regenerate(self) -> bool:
        return self.regenerate_mode.get() == "full"

    def get_effective_use_diff_mode(self) -> bool:
        return self.get_use_diff_mode() and not self.get_force_full_regenerate()

    def get_target_stations(self) -> list[tuple[str, str]]:
        if not self.use_selected_station_filter():
            return []
        pairs = self._get_selected_station_pairs()
        return sorted(pairs, key=lambda x: (x[0], x[1]))

    def _sync_regenerate_option_state(self) -> None:
        if not self._enabled:
            self.regenerate_mode_diff_radio.configure(state="disabled")
            self.regenerate_mode_full_radio.configure(state="disabled")
            return
        self.regenerate_mode_diff_radio.configure(state="normal")
        self.regenerate_mode_full_radio.configure(state="normal")

    def set_enabled(self, enabled: bool) -> None:
        self._enabled = enabled
        state = "normal" if enabled else "disabled"
        self.export_excel_check.configure(state=state)
        self.export_chart_check.configure(state=state)
        self.enable_parallel_check.configure(state=state)
        self.parallel_workers_spin.configure(state=state)
        self.recalc_parallel_workers_btn.configure(state=state)
        self.apply_parallel_workers_btn.configure(state=state)
        self._sync_regenerate_option_state()
        self.selection_filter_check.configure(state=state)
        self.clear_selection_btn.configure(state=state)
        self._apply_scan_button_state()

    def refresh_layout(self) -> None:
        self.update_idletasks()
        self.tree.update_idletasks()


# =========================================================================
# タブ3: 期間CSV出力
# =========================================================================


class PeriodCsvExportTab(ttk.Frame):
    _DEFAULT_PARQUET_DIR_PRIMARY = (
        r"Z:\1175D109_大阪狭山市におけるため池を考慮した浸水シミュレーション構築に関する検討業務"
        r"\50_作業\01_ochiai_temp\06_1974年-2025年_取得整形結果\parquet"
    )
    _DEFAULT_PARQUET_DIR_SECONDARY = (
        r"Z:\1175D109_大阪狭山市におけるため池を考慮した浸水シミュレーション構築に関する検討業務"
        r"\50_作業\01_ochiai_temp\07_1974-2025-mizmizDB\parquet"
    )

    def __init__(self, parent: ttk.Notebook) -> None:
        super().__init__(parent, padding=10)
        self.columnconfigure(0, weight=1)
        self.rowconfigure(1, weight=1)
        self._enabled = True
        self._scan_running = False
        self._pending_scan: list[tuple[str, str]] | None = None
        self._entry_by_item_id: dict[str, dict[str, object]] = {}
        self._multi_targets: list[RainfallParquetPeriodExportTarget] = []
        self._jma_name_by_block: dict[str, str] = {}
        self._waterinfo_name_by_id: dict[str, str] = {}
        self._load_station_name_index()
        self._build()
        self._update_multi_target_summary()
        self._refresh_target_preview()

    def _load_station_name_index(self) -> None:
        jma_json = Path(__file__).resolve().parents[1] / "resources" / "jma_station_index.json"
        if jma_json.exists():
            try:
                with jma_json.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                by_block = data.get("by_block_no", {})
                if isinstance(by_block, dict):
                    for block_no, rows in by_block.items():
                        if not isinstance(rows, list) or not rows:
                            continue
                        name = str(rows[0].get("station_name") or "").strip()
                        if name:
                            self._jma_name_by_block[str(block_no)] = name
            except Exception:
                pass

        wi_json = Path(__file__).resolve().parents[1] / "resources" / "waterinfo_station_index.json"
        if wi_json.exists():
            try:
                with wi_json.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                by_station_id = data.get("by_station_id", {})
                if isinstance(by_station_id, dict):
                    for sid, row in by_station_id.items():
                        if not isinstance(row, dict):
                            continue
                        name = str(row.get("station_name") or "").strip()
                        if name:
                            self._waterinfo_name_by_id[str(sid)] = name
            except Exception:
                pass

    def _build(self) -> None:
        dir_frame = ttk.LabelFrame(self, text="Parquet入力", padding=6, style="Soft.TLabelframe")
        dir_frame.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        dir_frame.columnconfigure(0, weight=0, minsize=90)
        dir_frame.columnconfigure(1, weight=1, minsize=420)
        dir_frame.columnconfigure(2, weight=0, minsize=72)
        dir_frame.columnconfigure(3, weight=0, minsize=72)
        self.parquet_dir_primary = tk.StringVar(value=self._DEFAULT_PARQUET_DIR_PRIMARY)
        self.parquet_dir_secondary = tk.StringVar(value=self._DEFAULT_PARQUET_DIR_SECONDARY)
        ttk.Label(dir_frame, text="ParquetフォルダA").grid(row=0, column=0, sticky="w", padx=(0, 8))
        self.parquet_dir_entry = ttk.Entry(dir_frame, textvariable=self.parquet_dir_primary)
        self.parquet_dir_entry.grid(row=0, column=1, sticky="ew")
        self.parquet_browse_btn = ttk.Button(
            dir_frame, text="参照...", command=lambda: self._browse_parquet_dir(self.parquet_dir_primary), style="StationColor.TButton",
        )
        self.parquet_browse_btn.grid(row=0, column=2, padx=(6, 0))
        ttk.Label(dir_frame, text="ParquetフォルダB").grid(row=1, column=0, sticky="w", padx=(0, 8), pady=(6, 0))
        self.parquet_dir_entry_2 = ttk.Entry(dir_frame, textvariable=self.parquet_dir_secondary)
        self.parquet_dir_entry_2.grid(row=1, column=1, sticky="ew", pady=(6, 0))
        self.parquet_browse_btn_2 = ttk.Button(
            dir_frame, text="参照...", command=lambda: self._browse_parquet_dir(self.parquet_dir_secondary), style="StationColor.TButton",
        )
        self.parquet_browse_btn_2.grid(row=1, column=2, padx=(6, 0), pady=(6, 0))
        self.parquet_scan_btn = ttk.Button(
            dir_frame, text="スキャン", command=self._manual_scan, style="StationColor.TButton",
        )
        self.parquet_scan_btn.grid(row=0, column=3, rowspan=2, padx=(6, 0))
        self.scan_status = tk.StringVar(value="")
        ttk.Label(dir_frame, textvariable=self.scan_status, foreground="#666").grid(
            row=2, column=0, columnspan=4, sticky="w", pady=(4, 0),
        )

        station_frame = ttk.LabelFrame(self, text="Parquet観測所一覧", padding=6, style="Soft.TLabelframe")
        station_frame.grid(row=1, column=0, sticky="nsew", pady=(0, 6))
        station_frame.columnconfigure(0, weight=1)
        station_frame.rowconfigure(0, weight=1)
        columns = ("source", "station_code", "station_name", "years")
        self.tree = ttk.Treeview(station_frame, columns=columns, show="headings", height=12, selectmode="none")
        self.tree.heading("source", text="データ元")
        self.tree.heading("station_code", text="観測所コード")
        self.tree.heading("station_name", text="観測所")
        self.tree.heading("years", text="利用可能年")
        self.tree.column("source", width=90, anchor="center")
        self.tree.column("station_code", width=140, anchor="center")
        self.tree.column("station_name", width=180, anchor="w")
        self.tree.column("years", width=220, anchor="w")
        self.tree.grid(row=0, column=0, sticky="nsew")
        tree_scroll = ttk.Scrollbar(station_frame, orient="vertical", command=self.tree.yview)
        tree_scroll.grid(row=0, column=1, sticky="ns")
        self.tree.configure(yscrollcommand=tree_scroll.set)
        form_frame = ttk.LabelFrame(self, text="CSV出力設定", padding=6, style="Soft.TLabelframe")
        form_frame.grid(row=2, column=0, sticky="ew")
        form_frame.columnconfigure(1, weight=1)

        self.multi_target_summary = tk.StringVar(value="複数観測所設定: 未使用")
        ttk.Label(form_frame, text="設定状況").grid(row=0, column=0, sticky="w", padx=(0, 12))
        ttk.Label(form_frame, textvariable=self.multi_target_summary, foreground="#334155").grid(
            row=0, column=1, sticky="w"
        )
        self.multi_target_btn = ttk.Button(
            form_frame,
            text="複数観測所設定...",
            command=self._open_multi_target_dialog,
            style="StationColor.TButton",
        )
        self.multi_target_btn.grid(row=1, column=0, sticky="w", pady=(8, 0))
        self.clear_multi_target_btn = ttk.Button(
            form_frame,
            text="設定をクリア",
            command=self._clear_multi_targets,
            style="StationColor.TButton",
        )
        self.clear_multi_target_btn.grid(row=1, column=1, sticky="w", pady=(8, 0))

        self.output_hint = tk.StringVar(value="CSV出力先は下部の共通設定を使用します。")
        ttk.Label(form_frame, text="出力先").grid(row=2, column=0, sticky="w", padx=(0, 12), pady=(8, 0))
        ttk.Label(form_frame, textvariable=self.output_hint, foreground="#334155").grid(
            row=2, column=1, sticky="w", pady=(8, 0)
        )

        self.action_hint = tk.StringVar(value="スキャン後に複数観測所設定を開き、対象観測所と開始日・終了日を設定してください。")
        ttk.Label(form_frame, textvariable=self.action_hint, foreground="#666", wraplength=340).grid(
            row=3, column=0, columnspan=2, sticky="w", pady=(10, 0)
        )

        target_preview_frame = ttk.LabelFrame(self, text="設定済み出力対象", padding=6, style="Soft.TLabelframe")
        target_preview_frame.grid(row=3, column=0, sticky="nsew", pady=(6, 0))
        target_preview_frame.columnconfigure(0, weight=1)
        target_preview_frame.rowconfigure(0, weight=1)
        preview_columns = ("source", "station_code", "station_name", "start_date", "end_date")
        self.target_preview_tree = ttk.Treeview(
            target_preview_frame,
            columns=preview_columns,
            show="headings",
            height=8,
            selectmode="none",
        )
        for key, text, width in (
            ("source", "データ元", 90),
            ("station_code", "観測所コード", 130),
            ("station_name", "観測所", 180),
            ("start_date", "開始日", 120),
            ("end_date", "終了日", 120),
        ):
            self.target_preview_tree.heading(key, text=text)
            self.target_preview_tree.column(key, width=width, anchor="center" if key != "station_name" else "w")
        self.target_preview_tree.grid(row=0, column=0, sticky="nsew")
        preview_scroll = ttk.Scrollbar(target_preview_frame, orient="vertical", command=self.target_preview_tree.yview)
        preview_scroll.grid(row=0, column=1, sticky="ns")
        self.target_preview_tree.configure(yscrollcommand=preview_scroll.set)

    def _browse_parquet_dir(self, target_var: tk.StringVar) -> None:
        path = filedialog.askdirectory()
        if path:
            target_var.set(path)

    def _manual_scan(self) -> None:
        targets = self.get_parquet_dirs()
        if not targets:
            messagebox.showinfo("スキャン", "Parquet入力ディレクトリを1つ以上指定してください。")
            return
        self.scan_current_inputs()

    def get_parquet_dirs(self) -> list[tuple[str, str]]:
        rows: list[tuple[str, str]] = []
        first = self.parquet_dir_primary.get().strip()
        second = self.parquet_dir_secondary.get().strip()
        if first:
            rows.append(("A", first))
        if second and second != first:
            rows.append(("B", second))
        return rows

    def scan_current_inputs(self) -> None:
        targets = self.get_parquet_dirs()
        if not targets:
            return
        if self._scan_running:
            self._pending_scan = list(targets)
            self.scan_status.set("スキャン待機中...")
            return
        self._start_scan(targets)

    def _start_scan(self, parquet_targets: list[tuple[str, str]]) -> None:
        self._scan_running = True
        self.scan_status.set("スキャン中...")

        def worker() -> None:
            try:
                payload_entries: list[tuple[str, str, object]] = []
                for input_label, parquet_dir in parquet_targets:
                    entries = scan_parquet_dir(parquet_dir)
                    payload_entries.extend((input_label, parquet_dir, entry) for entry in entries)
                payload = ("ok", payload_entries)
            except Exception as exc:  # noqa: BLE001
                payload = ("error", f"{type(exc).__name__}: {exc}")
            try:
                self.after(0, self._finish_scan, payload)
            except tk.TclError:
                pass

        threading.Thread(target=worker, daemon=True).start()

    def _finish_scan(self, payload: tuple) -> None:
        self._scan_running = False
        try:
            kind = payload[0]
            if kind == "ok":
                _kind, entries = payload
                self._refresh_table(entries)
            else:
                _kind, message = payload
                self.scan_status.set("スキャン失敗")
                messagebox.showerror("スキャンエラー", str(message))
        finally:
            next_scan = self._pending_scan
            self._pending_scan = None
            if next_scan is not None:
                self._start_scan(next_scan)

    def _refresh_table(self, entries) -> None:
        for item in self.tree.get_children():
            self.tree.delete(item)
        self._entry_by_item_id.clear()

        station_index: dict[tuple[str, str, str], dict[str, object]] = {}
        for input_label, parquet_dir, entry in entries:
            key = (str(input_label), str(entry.source), str(entry.station_key))
            row = station_index.setdefault(
                key,
                {
                    "input_label": str(input_label),
                    "parquet_dir": str(parquet_dir),
                    "source": str(entry.source),
                    "station_key": str(entry.station_key),
                    "display_station_code": self._to_display_station_code(str(entry.source), str(entry.station_key)),
                    "station_name": "",
                    "years": [],
                },
            )
            row["years"].append(int(entry.year))

        for row in station_index.values():
            source = str(row["source"])
            station_code = str(row["display_station_code"])
            station_name = self._resolve_display_station_name(source, station_code)
            row["station_name"] = station_name
            source_label = "気象庁" if source == "jma" else "水文水質DB"
            years = sorted(set(int(year) for year in row["years"]))
            years_text = f"{years[0]}-{years[-1]}" if years else "—"
            iid = self.tree.insert("", "end", values=(source_label, station_code, station_name, years_text))
            self._entry_by_item_id[iid] = row

        self.scan_status.set(f"{len(station_index)}観測所")

    @staticmethod
    def _to_display_station_code(source: str, station_key: str) -> str:
        if source == "jma" and "_" in station_key:
            return station_key.split("_", 1)[1]
        return station_key

    def _resolve_display_station_name(self, source: str, station_code: str) -> str:
        if source == "jma":
            return self._jma_name_by_block.get(station_code, "")
        return self._waterinfo_name_by_id.get(station_code, "")

    def build_batch_input(self) -> RainfallParquetPeriodBatchExportInput:
        parquet_dirs = self.get_parquet_dirs()
        default_parquet_dir = parquet_dirs[0][1] if parquet_dirs else ""
        app = self.winfo_toplevel()
        output_dir = app.output_dir.get().strip() if hasattr(app, "output_dir") else ""  # type: ignore[union-attr]
        if not self._multi_targets:
            raise ValueError("複数観測所設定がありません。")
        if not output_dir:
            raise ValueError("CSV出力ディレクトリを指定してください。")
        for target in self._multi_targets:
            if not str(target.parquet_dir or "").strip() and not default_parquet_dir:
                raise ValueError(f"{target.display_station_code or target.station_key} の Parquet 入力元が未設定です。")
            if not str(target.start_date or "").strip():
                raise ValueError(f"{target.display_station_code or target.station_key} の開始日が未設定です。")
            if not str(target.end_date or "").strip():
                raise ValueError(f"{target.display_station_code or target.station_key} の終了日が未設定です。")
        return RainfallParquetPeriodBatchExportInput(
            parquet_dir=default_parquet_dir,
            output_dir=output_dir,
            targets=list(self._multi_targets),
        )

    def _update_multi_target_summary(self) -> None:
        count = len(self._multi_targets)
        if count == 0:
            self.multi_target_summary.set("複数観測所設定: 未使用")
        else:
            self.multi_target_summary.set(f"複数観測所設定: {count}件")

    def _available_station_rows(self) -> list[dict[str, object]]:
        rows: list[dict[str, object]] = []
        for row in self._entry_by_item_id.values():
            years = sorted(set(int(year) for year in row.get("years", [])))
            rows.append(
                {
                    "input_label": str(row.get("input_label", "")),
                    "parquet_dir": str(row.get("parquet_dir", "")),
                    "source": str(row.get("source", "")),
                    "station_key": str(row.get("station_key", "")),
                    "display_station_code": str(row.get("display_station_code", "")),
                    "station_name": str(row.get("station_name", "")),
                    "available_years": years,
                }
            )
        rows.sort(key=lambda item: (item["input_label"], item["source"], item["display_station_code"]))
        return rows

    def _open_multi_target_dialog(self) -> None:
        rows = self._available_station_rows()
        if not rows:
            messagebox.showinfo("複数観測所設定", "先に Parquet をスキャンしてください。")
            return
        dialog = PeriodTargetConfigDialog(self, rows, self._multi_targets)
        self.wait_window(dialog)
        if dialog.result is not None:
            self._multi_targets = dialog.result
            self._update_multi_target_summary()
            self._refresh_target_preview()

    def _clear_multi_targets(self) -> None:
        self._multi_targets = []
        self._update_multi_target_summary()
        self._refresh_target_preview()

    def _refresh_target_preview(self) -> None:
        for item_id in self.target_preview_tree.get_children():
            self.target_preview_tree.delete(item_id)
        for idx, target in enumerate(self._multi_targets):
            source_label = "気象庁" if target.source == "jma" else "水文水質DB"
            self.target_preview_tree.insert(
                "",
                "end",
                iid=f"preview::{idx}",
                values=(
                    source_label,
                    target.display_station_code or target.station_key,
                    target.station_name,
                    str(target.start_date or ""),
                    str(target.end_date or ""),
                ),
            )

    def set_enabled(self, enabled: bool) -> None:
        self._enabled = enabled
        state = "normal" if enabled else "disabled"
        self.parquet_dir_entry.configure(state=state)
        self.parquet_dir_entry_2.configure(state=state)
        self.parquet_browse_btn.configure(state=state)
        self.parquet_browse_btn_2.configure(state=state)
        self.parquet_scan_btn.configure(state=state)
        self.multi_target_btn.configure(state=state)
        self.clear_multi_target_btn.configure(state=state)

    def refresh_layout(self) -> None:
        self.update_idletasks()
        self.tree.update_idletasks()


class PeriodTargetConfigDialog(tk.Toplevel):
    def __init__(
        self,
        parent: tk.Misc,
        available_rows: list[dict[str, object]],
        initial_targets: list[RainfallParquetPeriodExportTarget],
    ) -> None:
        super().__init__(parent)
        self.title("複数観測所設定")
        self.geometry("1320x760")
        self.minsize(1120, 660)
        self.transient(parent.winfo_toplevel())
        self.grab_set()
        self.result: list[RainfallParquetPeriodExportTarget] | None = None
        self._available_rows = available_rows
        self._targets: list[RainfallParquetPeriodExportTarget] = [
            RainfallParquetPeriodExportTarget(
                parquet_dir=target.parquet_dir,
                source=target.source,
                station_key=target.station_key,
                start_date=target.start_date,
                end_date=target.end_date,
                station_name=target.station_name,
                display_station_code=target.display_station_code,
                available_years=list(target.available_years),
            )
            for target in initial_targets
        ]
        self._target_by_item_id: dict[str, RainfallParquetPeriodExportTarget] = {}
        self.columnconfigure(0, weight=5, minsize=500)
        self.columnconfigure(1, weight=6, minsize=560)
        self.rowconfigure(0, weight=1)
        self._build()
        self._refresh_target_tree()

    def _build(self) -> None:
        available_frame = ttk.LabelFrame(self, text="スキャン済み観測所", padding=6, style="Soft.TLabelframe")
        available_frame.grid(row=0, column=0, sticky="nsew", padx=(10, 6), pady=10)
        available_frame.columnconfigure(0, weight=1)
        available_frame.rowconfigure(0, weight=1)
        available_frame.rowconfigure(1, weight=0)
        self.available_tree = ttk.Treeview(
            available_frame,
            columns=("source", "station_code", "station_name", "years"),
            show="headings",
            height=18,
            selectmode="extended",
        )
        for key, text, width in (
            ("source", "データ元", 90),
            ("station_code", "観測所コード", 140),
            ("station_name", "観測所", 180),
            ("years", "利用可能年", 220),
        ):
            self.available_tree.heading(key, text=text)
            self.available_tree.column(key, width=width, anchor="w" if key in {"station_name", "years"} else "center")
        self.available_tree.grid(row=0, column=0, sticky="nsew")
        avail_scroll = ttk.Scrollbar(available_frame, orient="vertical", command=self.available_tree.yview)
        avail_scroll.grid(row=0, column=1, sticky="ns")
        avail_xscroll = ttk.Scrollbar(available_frame, orient="horizontal", command=self.available_tree.xview)
        avail_xscroll.grid(row=1, column=0, sticky="ew", pady=(4, 0))
        self.available_tree.configure(yscrollcommand=avail_scroll.set, xscrollcommand=avail_xscroll.set)
        for row in self._available_rows:
            source_label = "気象庁" if str(row["source"]) == "jma" else "水文水質DB"
            years = list(row.get("available_years", []))
            years_text = f"{years[0]}-{years[-1]}" if years else "—"
            self.available_tree.insert(
                "",
                "end",
                values=(source_label, row["display_station_code"], row["station_name"], years_text),
                iid=f"{row['input_label']}::{row['source']}::{row['station_key']}",
            )

        btn_col = ttk.Frame(self)
        btn_col.grid(row=0, column=1, sticky="nsew", padx=(6, 10), pady=10)
        btn_col.columnconfigure(0, weight=1)
        btn_col.rowconfigure(1, weight=1)

        top_btns = ttk.Frame(btn_col)
        top_btns.grid(row=0, column=0, sticky="ew")
        self.add_selected_btn = ttk.Button(
            top_btns, text="選択を追加", command=self._add_selected, style="StationColor.TButton",
        )
        self.add_selected_btn.pack(side="left")
        self.remove_selected_btn = ttk.Button(
            top_btns, text="設定から削除", command=self._remove_selected, style="StationColor.TButton",
        )
        self.remove_selected_btn.pack(side="left", padx=(8, 0))
        self.import_btn = ttk.Button(
            top_btns, text="CSV読込", command=self._import_csv, style="StationColor.TButton",
        )
        self.import_btn.pack(side="left", padx=(16, 0))
        self.export_btn = ttk.Button(
            top_btns, text="CSV保存", command=self._export_csv, style="StationColor.TButton",
        )
        self.export_btn.pack(side="left", padx=(8, 0))

        target_frame = ttk.LabelFrame(btn_col, text="出力対象設定", padding=6, style="Soft.TLabelframe")
        target_frame.grid(row=1, column=0, sticky="nsew", pady=(8, 0))
        target_frame.columnconfigure(0, weight=1)
        target_frame.rowconfigure(0, weight=1)
        target_frame.rowconfigure(1, weight=0)
        self.target_tree = ttk.Treeview(
            target_frame,
            columns=("source", "station_code", "station_name", "start_date", "end_date"),
            show="headings",
            height=14,
            selectmode="browse",
        )
        for key, text, width in (
            ("source", "データ元", 90),
            ("station_code", "観測所コード", 130),
            ("station_name", "観測所", 180),
            ("start_date", "開始日", 120),
            ("end_date", "終了日", 120),
        ):
            self.target_tree.heading(key, text=text)
            self.target_tree.column(key, width=width, anchor="center" if key != "station_name" else "w")
        self.target_tree.grid(row=0, column=0, sticky="nsew")
        target_scroll = ttk.Scrollbar(target_frame, orient="vertical", command=self.target_tree.yview)
        target_scroll.grid(row=0, column=1, sticky="ns")
        target_xscroll = ttk.Scrollbar(target_frame, orient="horizontal", command=self.target_tree.xview)
        target_xscroll.grid(row=1, column=0, sticky="ew", pady=(4, 0))
        self.target_tree.configure(yscrollcommand=target_scroll.set, xscrollcommand=target_xscroll.set)
        self.target_tree.bind("<<TreeviewSelect>>", self._load_target_dates)

        edit_frame = ttk.Frame(btn_col)
        edit_frame.grid(row=2, column=0, sticky="ew", pady=(8, 0))
        ttk.Label(edit_frame, text="開始日").pack(side="left")
        self.start_year_var = tk.StringVar(value="")
        self.start_month_var = tk.StringVar(value="")
        self.start_day_var = tk.StringVar(value="")
        self.start_year_combo = ttk.Combobox(
            edit_frame,
            textvariable=self.start_year_var,
            width=5,
            state="readonly",
            values=[],
        )
        self.start_year_combo.pack(side="left", padx=(6, 2))
        ttk.Label(edit_frame, text="年").pack(side="left")
        self.start_month_combo = ttk.Combobox(
            edit_frame,
            textvariable=self.start_month_var,
            width=2,
            state="readonly",
            values=[f"{month:02d}" for month in range(1, 13)],
        )
        self.start_month_combo.pack(side="left", padx=(4, 2))
        ttk.Label(edit_frame, text="月").pack(side="left")
        self.start_day_combo = ttk.Combobox(
            edit_frame,
            textvariable=self.start_day_var,
            width=2,
            state="readonly",
            values=[f"{day:02d}" for day in range(1, 32)],
        )
        self.start_day_combo.pack(side="left", padx=(4, 2))
        ttk.Label(edit_frame, text="日").pack(side="left", padx=(0, 12))
        ttk.Label(edit_frame, text="終了日").pack(side="left")
        self.end_year_var = tk.StringVar(value="")
        self.end_month_var = tk.StringVar(value="")
        self.end_day_var = tk.StringVar(value="")
        self.end_year_combo = ttk.Combobox(
            edit_frame,
            textvariable=self.end_year_var,
            width=5,
            state="readonly",
            values=[],
        )
        self.end_year_combo.pack(side="left", padx=(6, 2))
        ttk.Label(edit_frame, text="年").pack(side="left")
        self.end_month_combo = ttk.Combobox(
            edit_frame,
            textvariable=self.end_month_var,
            width=2,
            state="readonly",
            values=[f"{month:02d}" for month in range(1, 13)],
        )
        self.end_month_combo.pack(side="left", padx=(4, 2))
        ttk.Label(edit_frame, text="月").pack(side="left")
        self.end_day_combo = ttk.Combobox(
            edit_frame,
            textvariable=self.end_day_var,
            width=2,
            state="readonly",
            values=[f"{day:02d}" for day in range(1, 32)],
        )
        self.end_day_combo.pack(side="left", padx=(4, 2))
        ttk.Label(edit_frame, text="日").pack(side="left", padx=(0, 12))
        self.apply_btn = ttk.Button(edit_frame, text="日付を反映", command=self._apply_dates, style="StationColor.TButton")
        self.apply_btn.pack(side="left")
        ttk.Label(edit_frame, text="開始日・終了日は必須", foreground="#666").pack(side="left", padx=(12, 0))

        bottom_btns = ttk.Frame(btn_col)
        bottom_btns.grid(row=3, column=0, sticky="e", pady=(12, 0))
        ttk.Button(bottom_btns, text="キャンセル", command=self._cancel, style="StationColor.TButton").pack(side="right")
        ttk.Button(bottom_btns, text="OK", command=self._confirm, style="StationColor.TButton").pack(side="right", padx=(0, 8))

    def _add_selected(self) -> None:
        selected_ids = self.available_tree.selection()
        for item_id in selected_ids:
            input_label, source, station_key = item_id.split("::", 2)
            row = next(
                (
                    row for row in self._available_rows
                    if row["input_label"] == input_label and row["source"] == source and row["station_key"] == station_key
                ),
                None,
            )
            if row is None:
                continue
            self._targets.append(
                RainfallParquetPeriodExportTarget(
                    parquet_dir=str(row.get("parquet_dir", "")),
                    source=source,
                    station_key=station_key,
                    start_date="",
                    end_date="",
                    station_name=str(row.get("station_name", "")),
                    display_station_code=str(row.get("display_station_code", "")),
                    available_years=list(row.get("available_years", [])),
                )
            )
        self._refresh_target_tree()

    def _remove_selected(self) -> None:
        selection = self.target_tree.selection()
        if not selection:
            return
        item_id = selection[0]
        target = self._target_by_item_id.get(item_id)
        if target is None:
            return
        self._targets = [current for current in self._targets if current is not target]
        self._refresh_target_tree()

    def _refresh_target_tree(self) -> None:
        for item_id in self.target_tree.get_children():
            self.target_tree.delete(item_id)
        self._target_by_item_id.clear()
        for idx, target in enumerate(self._targets):
            source_label = "気象庁" if target.source == "jma" else "水文水質DB"
            item_id = f"target::{idx}"
            self.target_tree.insert(
                "",
                "end",
                iid=item_id,
                values=(
                    source_label,
                    target.display_station_code or target.station_key,
                    target.station_name,
                    str(target.start_date or ""),
                    str(target.end_date or ""),
                ),
            )
            self._target_by_item_id[item_id] = target

    def _load_target_dates(self, _event: tk.Event | None = None) -> None:  # type: ignore[type-arg]
        selection = self.target_tree.selection()
        if not selection:
            self._set_year_choices([])
            self._set_split_date_vars(None, None)
            return
        target = self._target_by_item_id.get(selection[0])
        if target is None:
            return
        self._set_year_choices(list(target.available_years))
        self._set_split_date_vars(str(target.start_date or ""), str(target.end_date or ""))

    def _apply_dates(self) -> None:
        selection = self.target_tree.selection()
        if not selection:
            messagebox.showinfo("複数観測所設定", "出力対象設定から1行選択してください。")
            return
        target = self._target_by_item_id.get(selection[0])
        if target is None:
            return
        start_text = self._compose_split_date_text(
            self.start_year_var.get(),
            self.start_month_var.get(),
            self.start_day_var.get(),
            "開始日",
        )
        end_text = self._compose_split_date_text(
            self.end_year_var.get(),
            self.end_month_var.get(),
            self.end_day_var.get(),
            "終了日",
        )
        try:
            for label, value in (("開始日", start_text), ("終了日", end_text)):
                datetime.strptime(value, "%Y-%m-%d")
            if start_text > end_text:
                raise ValueError("開始日は終了日以前で入力してください。")
        except ValueError as exc:
            messagebox.showerror("複数観測所設定", str(exc))
            return
        target.start_date = start_text
        target.end_date = end_text
        self._refresh_target_tree()
        self.target_tree.selection_set(selection[0])
        self._load_target_dates()

    def _import_csv(self) -> None:
        path = filedialog.askopenfilename(filetypes=[("CSV", "*.csv"), ("All", "*.*")])
        if not path:
            return
        imported = load_period_targets_csv(path)
        available_lookup = {
            (str(row["parquet_dir"]), str(row["source"]), str(row["station_key"])): row
            for row in self._available_rows
        }
        normalized: list[RainfallParquetPeriodExportTarget] = []
        for target in imported:
            row = available_lookup.get((target.parquet_dir, target.source, target.station_key))
            if row is not None:
                target.station_name = str(row.get("station_name", target.station_name))
                target.display_station_code = str(row.get("display_station_code", target.display_station_code))
                target.available_years = list(row.get("available_years", []))
            if not str(target.start_date or "").strip() or not str(target.end_date or "").strip():
                messagebox.showerror("複数観測所設定", "CSV読込では開始日・終了日が必須です。")
                return
            normalized.append(target)
        self._targets = normalized
        self._refresh_target_tree()

    def _export_csv(self) -> None:
        if not self._targets:
            messagebox.showinfo("複数観測所設定", "保存する設定がありません。")
            return
        path = filedialog.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV", "*.csv"), ("All", "*.*")],
        )
        if not path:
            return
        export_period_targets_csv(path, self._targets)

    def _confirm(self) -> None:
        for target in self._targets:
            if not str(target.start_date or "").strip() or not str(target.end_date or "").strip():
                messagebox.showerror(
                    "複数観測所設定",
                    f"{target.display_station_code or target.station_key} の開始日・終了日を設定してください。",
                )
                return
        self.result = [
            RainfallParquetPeriodExportTarget(
                parquet_dir=target.parquet_dir,
                source=target.source,
                station_key=target.station_key,
                start_date=target.start_date,
                end_date=target.end_date,
                station_name=target.station_name,
                display_station_code=target.display_station_code,
                available_years=list(target.available_years),
            )
            for target in self._targets
        ]
        self.destroy()


    @staticmethod
    def _split_date_text(value: str | None) -> tuple[str, str, str]:
        text = str(value or "").strip()
        if not text:
            return "", "", ""
        try:
            parsed = datetime.strptime(text, "%Y-%m-%d")
        except ValueError:
            return "", "", ""
        return f"{parsed.year:04d}", f"{parsed.month:02d}", f"{parsed.day:02d}"

    def _set_split_date_vars(self, start_text: str | None, end_text: str | None) -> None:
        start_year, start_month, start_day = self._split_date_text(start_text)
        end_year, end_month, end_day = self._split_date_text(end_text)
        self.start_year_var.set(start_year)
        self.start_month_var.set(start_month)
        self.start_day_var.set(start_day)
        self.end_year_var.set(end_year)
        self.end_month_var.set(end_month)
        self.end_day_var.set(end_day)

    def _set_year_choices(self, years: list[int]) -> None:
        values = [str(year) for year in sorted(set(int(year) for year in years))] if years else []
        self.start_year_combo.configure(values=values)
        self.end_year_combo.configure(values=values)

    @staticmethod
    def _compose_split_date_text(year_text: str, month_text: str, day_text: str, label: str) -> str:
        year_text = str(year_text).strip()
        month_text = str(month_text).strip()
        day_text = str(day_text).strip()
        if not year_text or not month_text or not day_text:
            raise ValueError(f"{label}の年・月・日をすべて入力してください。")
        if not year_text.isdigit() or not month_text.isdigit() or not day_text.isdigit():
            raise ValueError(f"{label}は数値で入力してください。")
        try:
            parsed = datetime(int(year_text), int(month_text), int(day_text))
        except ValueError as exc:
            raise ValueError(f"{label}の日付が不正です。") from exc
        return parsed.strftime("%Y-%m-%d")

    def _cancel(self) -> None:
        self.result = None
        self.destroy()

# =========================================================================
# エントリポイント
# =========================================================================


def main() -> int:
    app = RainfallGuiApp()
    app.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
