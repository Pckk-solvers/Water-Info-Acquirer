"""Rainfall GUI — タブ方式 (ttk.Notebook)。

「データ取得」タブと「整理・出力」タブで構成。
共通フッターに出力先フォルダ・実行/停止ボタン・ログを配置。
"""
from __future__ import annotations

import queue
import threading
import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from river_meta.rainfall.services import (
    RainfallGenerateInput,
    RainfallParquetPeriodBatchExportInput,
    RainfallRunInput,
    run_rainfall_analyze,
    run_rainfall_generate,
    run_rainfall_parquet_period_batch_export,
)
from pathlib import Path
from .collect_tab import CollectTab
from .generate_tab import GenerateTab
from .period_export_tab import PeriodCsvExportTab
from .support import supports_input_arg
from .tooltip import ToolTip
from water_info_acquirer.app_meta import get_module_title
from water_info_acquirer.navigation import build_navigation_menu


Event = tuple[str, object]


def _supports_generate_input_arg(arg_name: str) -> bool:
    """RainfallGenerateInput が指定引数を受け取れるかを判定する。"""
    return supports_input_arg(RainfallGenerateInput, arg_name)


class RainfallGuiApp(tk.Toplevel):
    def __init__(
        self,
        *,
        parent: tk.Misc,
        on_open_other=None,
        on_close=None,
        on_return_home=None,
        default_parquet_dir_primary: str = "",
        default_parquet_dir_secondary: str = "",
    ) -> None:
        super().__init__(parent)
        self.on_open_other = on_open_other
        self.on_close = on_close
        self.on_return_home = on_return_home
        self.title(get_module_title("rainfall", lang="jp"))
        self.geometry("1560x860")
        self.minsize(1360, 740)
        self._default_parquet_dir_primary = str(default_parquet_dir_primary or "")
        self._default_parquet_dir_secondary = str(default_parquet_dir_secondary or "")
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
        self.deiconify()
        self.lift()
        self.focus_force()

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

        self.config(
            menu=build_navigation_menu(
                self,
                current_app_key="rainfall",
                on_open_other=self._open_other,
                on_return_home=self._return_home,
            )
        )

        # --- Notebook (タブ) ---
        self.notebook = ttk.Notebook(root)
        self.notebook.grid(row=1, column=0, sticky="nsew")

        self.collect_tab = CollectTab(self.notebook)
        self.generate_tab = GenerateTab(self.notebook)
        self.period_export_tab = PeriodCsvExportTab(
            self.notebook,
            default_parquet_dir_primary=self._default_parquet_dir_primary,
            default_parquet_dir_secondary=self._default_parquet_dir_secondary,
        )
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

    def _open_other(self, app_key: str) -> None:
        if self.on_open_other:
            self.destroy()
            self.on_open_other(app_key)

    def _destroy_window(self) -> None:
        try:
            self.destroy()
        finally:
            if self.on_close:
                self.on_close()

    def _return_home(self) -> None:
        try:
            self.destroy()
        finally:
            if self.on_return_home:
                self.on_return_home()

    def _on_close(self) -> None:
        if not self._running:
            self._destroy_window()
            return
        if self._close_requested:
            force = messagebox.askyesno(
                "強制終了確認",
                "停止待機中です。強制終了しますか？\n処理途中の結果は失われる可能性があります。",
            )
            if force:
                self._destroy_window()
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
# エントリポイント
# =========================================================================


def show_rainfall(
    *,
    parent: tk.Misc,
    on_open_other=None,
    on_close=None,
    on_return_home=None,
    default_parquet_dir_primary: str = "",
    default_parquet_dir_secondary: str = "",
) -> RainfallGuiApp:
    return RainfallGuiApp(
        parent=parent,
        on_open_other=on_open_other,
        on_close=on_close,
        on_return_home=on_return_home,
        default_parquet_dir_primary=default_parquet_dir_primary,
        default_parquet_dir_secondary=default_parquet_dir_secondary,
    )


def main(
    *,
    default_parquet_dir_primary: str = "",
    default_parquet_dir_secondary: str = "",
) -> int:
    root = tk.Tk()
    root.withdraw()

    def _on_close():
        root.destroy()

    show_rainfall(
        parent=root,
        on_open_other=None,
        on_close=_on_close,
        default_parquet_dir_primary=default_parquet_dir_primary,
        default_parquet_dir_secondary=default_parquet_dir_secondary,
    )
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
