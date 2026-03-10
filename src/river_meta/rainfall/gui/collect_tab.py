from __future__ import annotations

import tkinter as tk
from datetime import datetime
from pathlib import Path
from tkinter import ttk

from river_meta.rainfall.services import RainfallRunInput

from .station_selector import StationSelector
from .support import supports_input_arg
from .tooltip import ToolTip


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

        src_frame = ttk.LabelFrame(self, text="取得元", padding=6, style="Soft.TLabelframe")
        src_frame.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 4))
        self.source = tk.StringVar(value="jma")
        for i, (label, token) in enumerate(
            [
                ("気象庁（JMA）", "jma"),
                ("水文水質データベース", "water_info"),
            ]
        ):
            rb = ttk.Radiobutton(src_frame, text=label, variable=self.source, value=token)
            rb.grid(row=0, column=i, sticky="w", padx=(0, 16))
            self._source_buttons.append(rb)
        self.source.trace_add("write", lambda *_: self._on_source_changed())

        year_frame = ttk.LabelFrame(self, text="対象年", padding=6, style="Soft.TLabelframe")
        year_frame.grid(row=1, column=0, columnspan=2, sticky="ew", pady=(0, 4))
        self.start_year = tk.StringVar(value=str(last_year))
        self.end_year = tk.StringVar(value=str(last_year))
        self.start_year_entry = ttk.Entry(year_frame, textvariable=self.start_year, width=8)
        self.start_year_entry.pack(side="left")
        ttk.Label(year_frame, text=" ～ ").pack(side="left")
        self.end_year_entry = ttk.Entry(year_frame, textvariable=self.end_year, width=8)
        self.end_year_entry.pack(side="left")

        order_frame = ttk.LabelFrame(self, text="取得順序", padding=6, style="Soft.TLabelframe")
        order_frame.grid(row=2, column=0, columnspan=2, sticky="ew", pady=(0, 4))
        self.collection_order = tk.StringVar(value="station_year")
        for i, (label, token) in enumerate(
            [
                ("観測所ごと（既定）", "station_year"),
                ("年ごと", "year_station"),
            ]
        ):
            rb = ttk.Radiobutton(order_frame, text=label, variable=self.collection_order, value=token)
            rb.grid(row=0, column=i, sticky="w", padx=(0, 16))
            self._order_buttons.append(rb)

        station_frame = ttk.LabelFrame(self, text="観測所指定", padding=6, style="Soft.TLabelframe")
        station_frame.grid(row=3, column=0, columnspan=2, sticky="nsew", pady=(0, 4))
        self.rowconfigure(3, weight=1)

        jma_json = Path(__file__).resolve().parents[2] / "resources" / "jma_station_index.json"
        wi_json = Path(__file__).resolve().parents[2] / "resources" / "waterinfo_station_index.json"

        self.station_selector = StationSelector(
            station_frame,
            jma_json_path=jma_json,
            waterinfo_json_path=wi_json,
        )
        self.station_selector.pack(fill="both", expand=True)

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
        if supports_input_arg(RainfallRunInput, "collection_order"):
            run_input_kwargs["collection_order"] = self.collection_order.get()

        return RainfallRunInput(**run_input_kwargs)


__all__ = ["CollectTab"]
