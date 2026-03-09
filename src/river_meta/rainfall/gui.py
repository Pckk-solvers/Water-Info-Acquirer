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
from river_meta.services.rainfall import (
    RainfallGenerateInput,
    RainfallRunInput,
    run_rainfall_analyze,
    run_rainfall_generate,
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
        self.notebook.add(self.collect_tab, text=" データ取得 ")
        self.notebook.add(self.generate_tab, text=" 整理・出力 ")
        self.notebook.bind("<<NotebookTabChanged>>", self._on_tab_changed)

        # --- 共通フッター: 出力先 + 実行ボタン ---
        footer = ttk.LabelFrame(root, text="共通設定", padding=6, style="Soft.TLabelframe")
        footer.grid(row=2, column=0, sticky="ew", pady=(8, 0))
        footer.columnconfigure(1, weight=1)

        self.output_dir = tk.StringVar(value="")
        ttk.Label(footer, text="出力先フォルダ").grid(row=0, column=0, sticky="w", padx=(0, 8))
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
            return "現在のParquetデータから整理・出力を実行します。"

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
        if self._running:
            return
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
        collect_config: RainfallRunInput | None = None
        if idx == 0:
            try:
                collect_config = self.collect_tab.build_run_input()
            except ValueError as exc:
                self.status.set("入力エラー")
                messagebox.showerror("入力エラー", str(exc))
                return
        else:
            if self.generate_tab.use_selected_station_filter() and not self.generate_tab.get_target_stations():
                self.status.set("入力エラー")
                messagebox.showerror(
                    "入力エラー",
                    "「選択行の観測所のみ出力」が有効ですが、対象行が選択されていません。",
                )
                return

        self._stop_event = threading.Event()
        self._running = True
        self._set_running_ui(True)
        self.status.set("実行中（データ取得）" if idx == 0 else "実行中（整理・出力）")

        if idx == 0:
            self._run_collect(output_dir, collect_config)
        else:
            self._run_generate(output_dir)

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
        else:
            self.run_btn.configure(text="整理・出力を実行")

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
# エントリポイント
# =========================================================================


def main() -> int:
    app = RainfallGuiApp()
    app.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
