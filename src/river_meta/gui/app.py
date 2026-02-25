from __future__ import annotations

import queue
import threading
import tkinter as tk
from tkinter import filedialog, messagebox, ttk
from typing import Callable

from river_meta.services.amedas import AmedasRunInput, DEFAULT_PDF_PATH, run_amedas_extract
from river_meta.services.gpkg import GpkgRunInput, run_csv_to_gpkg
from river_meta.services.river_meta import RiverMetaRunInput, run_river_meta
from river_meta.services.station_ids import StationIdsRunInput, run_station_ids_collect


Event = tuple[str, object]


class RiverMetaGuiApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("river-meta GUI")
        self.geometry("980x720")
        self.minsize(900, 620)
        self._event_queue: queue.Queue[Event] = queue.Queue()
        self._build_ui()
        self.after(120, self._drain_events)

    def _build_ui(self) -> None:
        root = ttk.Frame(self, padding=8)
        root.pack(fill="both", expand=True)
        root.columnconfigure(0, weight=1)
        root.rowconfigure(0, weight=1)

        notebook = ttk.Notebook(root)
        notebook.grid(row=0, column=0, sticky="nsew")

        station_ids_tab = ttk.Frame(notebook, padding=12)
        river_meta_tab = ttk.Frame(notebook, padding=12)
        gpkg_tab = ttk.Frame(notebook, padding=12)
        amedas_tab = ttk.Frame(notebook, padding=12)
        notebook.add(station_ids_tab, text="Station IDs")
        notebook.add(river_meta_tab, text="River Meta")
        notebook.add(gpkg_tab, text="CSV -> GPKG")
        notebook.add(amedas_tab, text="AMeDAS")

        for tab in (station_ids_tab, river_meta_tab, gpkg_tab, amedas_tab):
            tab.columnconfigure(1, weight=1)

        self._build_station_ids_tab(station_ids_tab)
        self._build_river_meta_tab(river_meta_tab)
        self._build_gpkg_tab(gpkg_tab)
        self._build_amedas_tab(amedas_tab)

        log_frame = ttk.LabelFrame(root, text="Log", padding=8)
        log_frame.grid(row=1, column=0, sticky="nsew", pady=(8, 0))
        log_frame.columnconfigure(0, weight=1)
        log_frame.rowconfigure(0, weight=1)

        self.log_text = tk.Text(log_frame, wrap="none", height=12)
        self.log_text.grid(row=0, column=0, sticky="nsew")
        scroll = ttk.Scrollbar(log_frame, command=self.log_text.yview)
        scroll.grid(row=0, column=1, sticky="ns")
        self.log_text.configure(yscrollcommand=scroll.set)

        clear_btn = ttk.Button(log_frame, text="Clear", command=self._clear_log)
        clear_btn.grid(row=1, column=0, sticky="e", pady=(8, 0))

    def _build_station_ids_tab(self, tab: ttk.Frame) -> None:
        self.station_ids_pref_list = tk.StringVar(value="大阪,兵庫,京都,奈良,和歌山")
        self.station_ids_item = tk.StringVar(value="雨量")
        self.station_ids_out_txt = tk.StringVar(value="data/id_inputs/station_ids.txt")
        self.station_ids_out_pref_csv = tk.StringVar(value="data/id_inputs/station_ids_by_pref.csv")

        self._entry_row(tab, 0, "都道府県 (CSV)", self.station_ids_pref_list)
        self._entry_row(tab, 1, "観測項目", self.station_ids_item)
        self._entry_row(
            tab,
            2,
            "出力 ID TXT",
            self.station_ids_out_txt,
            browse=lambda: self._select_save_file(self.station_ids_out_txt, [("Text", "*.txt"), ("All", "*.*")]),
        )
        self._entry_row(
            tab,
            3,
            "出力 Pref CSV",
            self.station_ids_out_pref_csv,
            browse=lambda: self._select_save_file(
                self.station_ids_out_pref_csv, [("CSV", "*.csv"), ("All", "*.*")]
            ),
        )

        run_btn = ttk.Button(tab, text="Run", command=lambda: self._run_station_ids(run_btn))
        run_btn.grid(row=4, column=0, sticky="w", pady=(12, 0))

    def _build_river_meta_tab(self, tab: ttk.Frame) -> None:
        self.river_meta_ids = tk.StringVar(value="")
        self.river_meta_id_file = tk.StringVar(value="")
        self.river_meta_out_dir_md = tk.StringVar(value="data/out_gui/md")
        self.river_meta_out_csv = tk.StringVar(value="data/out_gui/")
        self.river_meta_page_scan_max = tk.StringVar(value="2")
        self.river_meta_timeout = tk.StringVar(value="10")

        self._entry_row(tab, 0, "観測所ID (CSV)", self.river_meta_ids)
        self._entry_row(
            tab,
            1,
            "IDファイル",
            self.river_meta_id_file,
            browse=lambda: self._select_open_file(self.river_meta_id_file, [("Text/CSV", "*.txt *.csv"), ("All", "*.*")]),
        )
        self._entry_row(
            tab,
            2,
            "出力 Markdown Dir",
            self.river_meta_out_dir_md,
            browse=lambda: self._select_directory(self.river_meta_out_dir_md),
        )
        self._entry_row(
            tab,
            3,
            "出力 CSV",
            self.river_meta_out_csv,
            browse=lambda: self._select_save_file(self.river_meta_out_csv, [("CSV", "*.csv"), ("All", "*.*")]),
        )
        self._entry_row(tab, 4, "PAGE探索上限", self.river_meta_page_scan_max, width=12)
        self._entry_row(tab, 5, "Timeout 秒", self.river_meta_timeout, width=12)

        run_btn = ttk.Button(tab, text="Run", command=lambda: self._run_river_meta(run_btn))
        run_btn.grid(row=6, column=0, sticky="w", pady=(12, 0))

    def _build_gpkg_tab(self, tab: ttk.Frame) -> None:
        self.gpkg_in_csv = tk.StringVar(value="")
        self.gpkg_out_gpkg = tk.StringVar(value="data/out_gui/stations.gpkg")
        self.gpkg_out_epsg = tk.StringVar(value="4326")

        self._entry_row(
            tab,
            0,
            "入力 CSV",
            self.gpkg_in_csv,
            browse=lambda: self._select_open_file(self.gpkg_in_csv, [("CSV", "*.csv"), ("All", "*.*")]),
        )
        self._entry_row(
            tab,
            1,
            "出力 GPKG",
            self.gpkg_out_gpkg,
            browse=lambda: self._select_save_file(self.gpkg_out_gpkg, [("GPKG", "*.gpkg"), ("All", "*.*")]),
        )

        ttk.Label(tab, text="出力 EPSG").grid(row=2, column=0, sticky="w", pady=6, padx=(0, 8))
        epsg_values = ["4326"] + [str(value) for value in range(6669, 6689)]
        epsg_combo = ttk.Combobox(tab, textvariable=self.gpkg_out_epsg, values=epsg_values, width=10, state="readonly")
        epsg_combo.grid(row=2, column=1, sticky="w")

        run_btn = ttk.Button(tab, text="Run", command=lambda: self._run_gpkg(run_btn))
        run_btn.grid(row=3, column=0, sticky="w", pady=(12, 0))

    def _build_amedas_tab(self, tab: ttk.Frame) -> None:
        self.amedas_in_pdf = tk.StringVar(value=DEFAULT_PDF_PATH)
        self.amedas_out_csv = tk.StringVar(value="data/out/ame_master_kinki.csv")
        self.amedas_pref_list = tk.StringVar(value="大阪,兵庫,京都,奈良,和歌山")
        self.amedas_all_pref = tk.BooleanVar(value=False)

        self._entry_row(
            tab,
            0,
            "入力 PDF",
            self.amedas_in_pdf,
            browse=lambda: self._select_open_file(self.amedas_in_pdf, [("PDF", "*.pdf"), ("All", "*.*")]),
        )
        self._entry_row(
            tab,
            1,
            "出力 CSV",
            self.amedas_out_csv,
            browse=lambda: self._select_save_file(self.amedas_out_csv, [("CSV", "*.csv"), ("All", "*.*")]),
        )
        self._entry_row(tab, 2, "都道府県 (CSV)", self.amedas_pref_list)

        all_pref_check = ttk.Checkbutton(tab, text="全都道府県", variable=self.amedas_all_pref)
        all_pref_check.grid(row=3, column=0, sticky="w", pady=6)

        run_btn = ttk.Button(tab, text="Run", command=lambda: self._run_amedas(run_btn))
        run_btn.grid(row=4, column=0, sticky="w", pady=(12, 0))

    def _entry_row(
        self,
        parent: ttk.Frame,
        row: int,
        label: str,
        variable: tk.StringVar,
        *,
        browse: Callable[[], None] | None = None,
        width: int = 80,
    ) -> None:
        ttk.Label(parent, text=label).grid(row=row, column=0, sticky="w", pady=6, padx=(0, 8))
        entry = ttk.Entry(parent, textvariable=variable, width=width)
        entry.grid(row=row, column=1, sticky="ew", pady=6)
        if browse:
            ttk.Button(parent, text="...", width=3, command=browse).grid(row=row, column=2, sticky="w", padx=(8, 0))

    def _run_station_ids(self, button: ttk.Button) -> None:
        out_txt = self.station_ids_out_txt.get().strip()
        if not out_txt:
            messagebox.showerror("Input Error", "出力 ID TXT を指定してください。")
            return
        config = StationIdsRunInput(
            out=out_txt,
            out_pref_csv=self._optional(self.station_ids_out_pref_csv.get()),
            pref_list=self.station_ids_pref_list.get().strip(),
            item=self.station_ids_item.get().strip(),
        )
        self._run_async(
            button,
            "river-station-ids",
            lambda: run_station_ids_collect(config, log=self._log_from_worker),
            lambda result: messagebox.showinfo(
                "Completed",
                f"IDs: {result.total_ids}\nTXT: {result.output_txt}\nPref CSV: {result.output_pref_csv or '-'}",
            ),
        )

    def _run_river_meta(self, button: ttk.Button) -> None:
        station_ids = [item.strip() for item in self.river_meta_ids.get().split(",") if item.strip()]
        id_file = self._optional(self.river_meta_id_file.get())
        if not station_ids and not id_file:
            messagebox.showerror("Input Error", "観測所ID または IDファイル を指定してください。")
            return
        try:
            page_scan_max = int(self.river_meta_page_scan_max.get().strip())
            timeout_sec = float(self.river_meta_timeout.get().strip())
        except ValueError:
            messagebox.showerror("Input Error", "PAGE探索上限/Timeout の数値が不正です。")
            return

        config = RiverMetaRunInput(
            station_ids=station_ids or None,
            id_file=id_file,
            output_dir_md=self._optional(self.river_meta_out_dir_md.get()),
            output_csv_path=self._optional(self.river_meta_out_csv.get()),
            kinds=(2, 3),
            page_scan_max=page_scan_max,
            timeout_sec=timeout_sec,
        )
        self._run_async(
            button,
            "river-meta",
            lambda: run_river_meta(
                config,
                log_info=self._log_from_worker,
                log_warn=self._log_from_worker,
                log_error=self._log_from_worker,
            ),
            self._on_river_meta_done,
        )

    def _run_gpkg(self, button: ttk.Button) -> None:
        in_csv = self.gpkg_in_csv.get().strip()
        out_gpkg = self.gpkg_out_gpkg.get().strip()
        if not in_csv or not out_gpkg:
            messagebox.showerror("Input Error", "入力 CSV と 出力 GPKG を指定してください。")
            return
        try:
            out_epsg = int(self.gpkg_out_epsg.get())
        except ValueError:
            messagebox.showerror("Input Error", "EPSG が不正です。")
            return
        config = GpkgRunInput(in_csv=in_csv, out_gpkg=out_gpkg, out_epsg=out_epsg)
        self._run_async(
            button,
            "river-gpkg",
            lambda: run_csv_to_gpkg(config, log=self._log_from_worker),
            lambda result: messagebox.showinfo(
                "Completed",
                f"Input: {result.input_rows}\nValid: {result.valid_rows}\nInvalid: {result.invalid_rows}\nEPSG: {result.output_epsg}",
            ),
        )

    def _run_amedas(self, button: ttk.Button) -> None:
        in_pdf = self.amedas_in_pdf.get().strip()
        out_csv = self.amedas_out_csv.get().strip()
        if not in_pdf or not out_csv:
            messagebox.showerror("Input Error", "入力 PDF と 出力 CSV を指定してください。")
            return
        config = AmedasRunInput(
            in_pdf=in_pdf,
            out_csv=out_csv,
            pref_list=self.amedas_pref_list.get().strip(),
            all_pref=self.amedas_all_pref.get(),
        )
        self._run_async(
            button,
            "river-ame-master",
            lambda: run_amedas_extract(config, log=self._log_from_worker),
            lambda result: messagebox.showinfo(
                "Completed",
                f"Rows: {result.rows}\nTotal: {result.total_rows}\nSkipped: {result.skipped_rows}\nCSV: {result.output_csv}",
            ),
        )

    def _on_river_meta_done(self, result) -> None:
        message = (
            f"Exit: {result.exit_code}\n"
            f"Stations: {result.station_count}\n"
            f"Success: {result.success_count}\n"
            f"Failed: {result.failed_count}\n"
            f"CSV: {result.csv_path or '-'}"
        )
        if result.single_markdown:
            preview = result.single_markdown.replace("\n", " ")[:120]
            message = f"{message}\nMarkdown preview: {preview}..."
        messagebox.showinfo("Completed", message)

    def _run_async(
        self,
        button: ttk.Button,
        task_name: str,
        task: Callable[[], object],
        on_done: Callable[[object], None] | None = None,
    ) -> None:
        button.configure(state="disabled")
        self._append_log(f"[{task_name}] start")

        def worker() -> None:
            try:
                result = task()
            except Exception as exc:  # noqa: BLE001
                self._event_queue.put(("error", f"[{task_name}] {type(exc).__name__}: {exc}"))
            else:
                self._event_queue.put(("done", (on_done, result)))
            finally:
                self._event_queue.put(("enable", button))

        threading.Thread(target=worker, daemon=True).start()

    def _drain_events(self) -> None:
        try:
            while True:
                event, payload = self._event_queue.get_nowait()
                if event == "log":
                    self._append_log(str(payload))
                elif event == "error":
                    self._append_log(str(payload))
                    messagebox.showerror("Execution Error", str(payload))
                elif event == "done":
                    on_done, result = payload
                    if on_done is not None:
                        on_done(result)
                elif event == "enable":
                    payload.configure(state="normal")
        except queue.Empty:
            pass
        self.after(120, self._drain_events)

    def _log_from_worker(self, message: str) -> None:
        self._event_queue.put(("log", message))

    def _append_log(self, message: str) -> None:
        self.log_text.insert("end", message.rstrip() + "\n")
        self.log_text.see("end")

    def _clear_log(self) -> None:
        self.log_text.delete("1.0", "end")

    @staticmethod
    def _optional(value: str) -> str | None:
        stripped = value.strip()
        return stripped if stripped else None

    @staticmethod
    def _select_open_file(target: tk.StringVar, file_types: list[tuple[str, str]]) -> None:
        path = filedialog.askopenfilename(filetypes=file_types)
        if path:
            target.set(path)

    @staticmethod
    def _select_save_file(target: tk.StringVar, file_types: list[tuple[str, str]]) -> None:
        path = filedialog.asksaveasfilename(filetypes=file_types)
        if path:
            target.set(path)

    @staticmethod
    def _select_directory(target: tk.StringVar) -> None:
        path = filedialog.askdirectory()
        if path:
            target.set(path)
