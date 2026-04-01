"""Period CSV export UI for rainfall GUI."""

from __future__ import annotations

import json
import threading
import tkinter as tk
from datetime import datetime
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

from river_meta.rainfall.storage.parquet_store import scan_parquet_dir
from river_meta.rainfall.services import (
    RainfallParquetPeriodBatchExportInput,
    RainfallParquetPeriodExportTarget,
    export_period_targets_csv,
    load_period_targets_csv,
)


class PeriodCsvExportTab(ttk.Frame):
    def __init__(
        self,
        parent: ttk.Notebook,
        *,
        default_parquet_dir_primary: str = "",
        default_parquet_dir_secondary: str = "",
    ) -> None:
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
        self._default_parquet_dir_primary = str(default_parquet_dir_primary or "")
        self._default_parquet_dir_secondary = str(default_parquet_dir_secondary or "")
        self._load_station_name_index()
        self._build()
        self._update_multi_target_summary()
        self._refresh_target_preview()

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
        dir_frame = ttk.LabelFrame(self, text="Parquet入力", padding=6, style="Soft.TLabelframe")
        dir_frame.grid(row=0, column=0, sticky="ew", pady=(0, 6))
        dir_frame.columnconfigure(0, weight=0, minsize=90)
        dir_frame.columnconfigure(1, weight=1, minsize=420)
        dir_frame.columnconfigure(2, weight=0, minsize=72)
        dir_frame.columnconfigure(3, weight=0, minsize=72)
        self.parquet_dir_primary = tk.StringVar(value=self._default_parquet_dir_primary)
        self.parquet_dir_secondary = tk.StringVar(value=self._default_parquet_dir_secondary)
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
                    "station_name": str(getattr(entry, "station_name", "") or "").strip(),
                    "years": [],
                },
            )
            row["years"].append(int(entry.year))
            if not str(row.get("station_name", "")).strip():
                row["station_name"] = str(getattr(entry, "station_name", "") or "").strip()

        for row in station_index.values():
            source = str(row["source"])
            station_code = str(row["display_station_code"])
            station_name = self._resolve_display_station_name(source, station_code)
            if not station_name:
                station_name = str(row.get("station_name", "") or "").strip()
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
            selectmode="extended",
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

    def _selected_targets(self) -> list[RainfallParquetPeriodExportTarget]:
        targets: list[RainfallParquetPeriodExportTarget] = []
        for item_id in self.target_tree.selection():
            target = self._target_by_item_id.get(item_id)
            if target is not None:
                targets.append(target)
        return targets

    def _selected_target_item_ids(self) -> list[str]:
        return [item_id for item_id in self.target_tree.selection() if item_id in self._target_by_item_id]

    def _shared_available_years(self, targets: list[RainfallParquetPeriodExportTarget]) -> list[int]:
        if not targets:
            return []
        year_sets = [
            {int(year) for year in target.available_years}
            for target in targets
            if target.available_years
        ]
        if not year_sets:
            return []
        shared = set.intersection(*year_sets)
        return sorted(shared)

    def _clear_date_editor(self) -> None:
        self._set_year_choices([])
        self._set_split_date_vars(None, None)

    def _reset_selection_and_editor(self) -> None:
        for item_id in self.target_tree.selection():
            self.target_tree.selection_remove(item_id)
        self._clear_date_editor()

    def _merge_targets(
        self,
        existing: list[RainfallParquetPeriodExportTarget],
        incoming: list[RainfallParquetPeriodExportTarget],
    ) -> list[RainfallParquetPeriodExportTarget]:
        merged: dict[tuple[str, str, str, str, str], RainfallParquetPeriodExportTarget] = {}
        ordered_keys: list[tuple[str, str, str, str, str]] = []
        for target in existing:
            key = (
                str(target.parquet_dir),
                str(target.source),
                str(target.station_key),
                str(target.start_date or ""),
                str(target.end_date or ""),
            )
            merged[key] = target
            ordered_keys.append(key)
        for target in incoming:
            key = (
                str(target.parquet_dir),
                str(target.source),
                str(target.station_key),
                str(target.start_date or ""),
                str(target.end_date or ""),
            )
            if key not in merged:
                ordered_keys.append(key)
            merged[key] = target
        return [merged[key] for key in ordered_keys]

    @staticmethod
    def _copy_target_with_rows(
        target: RainfallParquetPeriodExportTarget,
        rows: list[dict[str, object]],
    ) -> RainfallParquetPeriodExportTarget:
        resolved_parquet_dir = str(target.parquet_dir or "").strip()
        resolved_station_name = str(target.station_name or "").strip()
        resolved_station_code = str(target.display_station_code or "").strip()
        merged_years: set[int] = set(int(year) for year in target.available_years)

        for row in rows:
            row_parquet_dir = str(row.get("parquet_dir", "")).strip()
            if not resolved_parquet_dir and row_parquet_dir:
                resolved_parquet_dir = row_parquet_dir
            row_station_name = str(row.get("station_name", "")).strip()
            if not resolved_station_name and row_station_name:
                resolved_station_name = row_station_name
            row_station_code = str(row.get("display_station_code", "")).strip()
            if not resolved_station_code and row_station_code:
                resolved_station_code = row_station_code
            merged_years.update(int(year) for year in row.get("available_years", []))

        return RainfallParquetPeriodExportTarget(
            parquet_dir=resolved_parquet_dir,
            source=target.source,
            station_key=target.station_key,
            start_date=target.start_date,
            end_date=target.end_date,
            station_name=resolved_station_name,
            display_station_code=resolved_station_code,
            available_years=sorted(merged_years),
        )

    def _choose_import_mode(self) -> str | None:
        if not self._targets:
            return "replace"
        answer = messagebox.askyesnocancel(
            "CSV読込",
            "現在の一覧をどう扱いますか？\n\nはい: CSVで置換\nいいえ: 現在の一覧へ追加\nキャンセル: 読込しない",
            parent=self,
        )
        if answer is None:
            return None
        return "replace" if answer else "append"

    def _remove_selected(self) -> None:
        selected_targets = self._selected_targets()
        if not selected_targets:
            return
        self._targets = [current for current in self._targets if current not in selected_targets]
        self._refresh_target_tree()
        self._clear_date_editor()

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
        selected_targets = self._selected_targets()
        if not selected_targets:
            self._clear_date_editor()
            return
        shared_years = self._shared_available_years(selected_targets)
        self._set_year_choices(shared_years)
        if len(selected_targets) != 1:
            self._set_split_date_vars(None, None)
            return
        target = selected_targets[0]
        self._set_split_date_vars(str(target.start_date or ""), str(target.end_date or ""))

    def _apply_dates(self) -> None:
        selected_targets = self._selected_targets()
        if not selected_targets:
            messagebox.showinfo("複数観測所設定", "出力対象設定から1行以上選択してください。")
            return
        shared_years = self._shared_available_years(selected_targets)
        if not shared_years:
            messagebox.showerror("複数観測所設定", "選択した観測所に共通する利用可能年がありません。")
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
        selected_item_ids = self._selected_target_item_ids()
        for target in selected_targets:
            target.start_date = start_text
            target.end_date = end_text
        self._refresh_target_tree()
        for item_id in selected_item_ids:
            if item_id in self._target_by_item_id:
                self.target_tree.selection_add(item_id)
        self._load_target_dates()

    def _import_csv(self) -> None:
        path = filedialog.askopenfilename(filetypes=[("CSV", "*.csv"), ("All", "*.*")])
        if not path:
            return
        mode = self._choose_import_mode()
        if mode is None:
            return
        imported = load_period_targets_csv(path)
        available_lookup = {
            (str(row["parquet_dir"]), str(row["source"]), str(row["station_key"])): row
            for row in self._available_rows
        }
        available_by_station: dict[tuple[str, str], list[dict[str, object]]] = {}
        for row in self._available_rows:
            key = (str(row["source"]), str(row["station_key"]))
            available_by_station.setdefault(key, []).append(row)
        normalized: list[RainfallParquetPeriodExportTarget] = []
        for target in imported:
            row = available_lookup.get((target.parquet_dir, target.source, target.station_key))
            if row is not None:
                target = self._copy_target_with_rows(target, [row])
            else:
                fallback_rows = available_by_station.get((target.source, target.station_key), [])
                if fallback_rows:
                    target = self._copy_target_with_rows(target, fallback_rows)
            if not str(target.start_date or "").strip() or not str(target.end_date or "").strip():
                messagebox.showerror("複数観測所設定", "CSV読込では開始日・終了日が必須です。")
                return
            normalized.append(target)
        if mode == "append":
            self._targets = self._merge_targets(self._targets, normalized)
        else:
            self._targets = normalized
        self._refresh_target_tree()
        self._reset_selection_and_editor()

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

