from __future__ import annotations

import json
import math
import os
import threading
import tkinter as tk
from pathlib import Path
from tkinter import messagebox, ttk

from river_meta.rainfall.storage.parquet_store import scan_parquet_dir

from .tooltip import ToolTip

try:
    import psutil
except Exception:  # noqa: BLE001
    psutil = None


class GenerateTab(ttk.Frame):
    """Parquetスキャン結果 + 出力オプション。"""

    def __init__(self, parent: ttk.Notebook) -> None:
        super().__init__(parent, padding=10)
        self.columnconfigure(0, weight=1)
        self.rowconfigure(2, weight=1)
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
        jma_json = Path(__file__).resolve().parents[2] / "resources" / "jma_station_index.json"
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

        wi_json = Path(__file__).resolve().parents[2] / "resources" / "waterinfo_station_index.json"
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
            mode_frame, text="差分更新（既定）", variable=self.regenerate_mode, value="diff"
        )
        self.regenerate_mode_diff_radio.pack(side="left", padx=(0, 16))
        self.regenerate_mode_full_radio = ttk.Radiobutton(
            mode_frame, text="全再生成", variable=self.regenerate_mode, value="full"
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
        self.enable_parallel_check = ttk.Checkbutton(parallel_frame, text="有効化", variable=self.enable_parallel)
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
            row=4, column=1, sticky="w", pady=(4, 0)
        )
        self._recalc_parallel_workers_recommendation()

        table_frame = ttk.LabelFrame(self, text="Parquetデータ状況", padding=3, style="Soft.TLabelframe")
        table_frame.grid(row=2, column=0, sticky="nsew", pady=(0, 4))
        table_frame.columnconfigure(0, weight=1)
        table_frame.rowconfigure(0, weight=1)

        columns = ("source", "station_code", "station_name", "year", "months", "complete")
        self.tree = ttk.Treeview(table_frame, columns=columns, show="headings", height=8, selectmode="extended")
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

        btn_frame = ttk.Frame(self)
        btn_frame.grid(row=3, column=0, sticky="ew")
        self.scan_btn = ttk.Button(btn_frame, text="スキャン更新", command=self._manual_scan, style="StationColor.TButton")
        self.scan_btn.pack(side="left")
        self.clear_selection_btn = ttk.Button(btn_frame, text="選択解除", command=self._clear_row_selection, style="StationColor.TButton")
        self.clear_selection_btn.pack(side="left", padx=(8, 0))
        self.use_selection_filter = tk.BooleanVar(value=False)
        self.selection_filter_check = ttk.Checkbutton(
            btn_frame,
            text="選択行の観測所のみ出力",
            variable=self.use_selection_filter,
            command=self._update_target_summary,
        )
        self.selection_filter_check.pack(side="left", padx=(12, 0))
        self.target_summary = tk.StringVar(value="対象: 全観測所（完全データのみ出力／不完全年は自動スキップ）")
        ttk.Label(btn_frame, textvariable=self.target_summary, foreground="#666").pack(side="left", padx=(12, 0))
        self.scan_status = tk.StringVar(value="")
        ttk.Label(btn_frame, textvariable=self.scan_status, foreground="#666").pack(side="right")
        self._setup_tooltips()

    def _setup_tooltips(self) -> None:
        self._tooltips.extend(
            [
                ToolTip(self.scan_btn, "出力先の parquet/ を再スキャンして一覧を更新します。"),
                ToolTip(self.selection_filter_check, "有効時は選択行の観測所だけを対象にします（どちらの場合も不完全年は自動スキップ）。"),
                ToolTip(self.regenerate_mode_full_radio, "更新方式を全再生成に切り替えます。"),
                ToolTip(self.enable_parallel_check, "有効時は Excel とグラフの出力で同じワーカー数を使って並列実行します。"),
                ToolTip(self.recalc_parallel_workers_btn, "PCのCPUコア数と利用可能RAMから推奨ワーカー数を再計算します。"),
                ToolTip(self.apply_parallel_workers_btn, "推奨ワーカー数をワーカー数欄に反映します。"),
            ]
        )

    def scan(self, output_dir: str, *, user_initiated: bool = False) -> None:
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
            output_dir = app.output_dir.get().strip()
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
            iid = self.tree.insert("", "end", values=(source_label, station_code, station_name, entry.year, month_count, status))
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
        self.target_summary.set(f"対象: {len(pairs)}観測所（選択行のみ／完全データのみ出力・不完全年は自動スキップ）")

    def _recalc_parallel_workers_recommendation(self) -> None:
        recommended, reason = self._calculate_recommended_parallel_workers()
        self._recommended_parallel_workers = max(self._parallel_workers_min, min(self._parallel_workers_max, int(recommended)))
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
