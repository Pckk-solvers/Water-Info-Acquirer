"""取得対象ごとの入力フォーム。要求の生成以外のI/Oはしない。"""

from datetime import date
from pathlib import Path
import tkinter as tk
from tkinter import ttk

from ..domain.dam import DamRequest
from .validation import InputValidator


class RegularConditions(ttk.Frame):
    def __init__(self, parent, *, single_sheet=False):
        super().__init__(parent, style="Water.Card.TFrame")
        self.mode = tk.StringVar(self, value="S")
        self.year_start = tk.StringVar(self, value=str(date.today().year))
        self.year_end = tk.StringVar(self, value=str(date.today().year))
        self.month_start = tk.StringVar(self, value="1月")
        self.month_end = tk.StringVar(self, value="12月")
        self.interval = tk.StringVar(self, value="時間値")
        self.single_sheet = tk.BooleanVar(self, value=single_sheet)
        self.columnconfigure(1, weight=1)
        ttk.Label(self, text="取得項目", style="Water.Card.TLabel").grid(row=0, column=0, sticky="w", padx=(0, 18))
        modes = ttk.Frame(self, style="Water.Card.TFrame")
        modes.grid(row=0, column=1, sticky="w")
        for label, value in (("水位", "S"), ("流量", "R"), ("雨量", "U")):
            ttk.Radiobutton(modes, text=label, value=value, variable=self.mode,
                            style="Water.TRadiobutton").pack(side="left", padx=(0, 20))
        ttk.Label(self, text="期間", style="Water.Card.TLabel").grid(row=1, column=0, sticky="w", pady=12)
        dates = ttk.Frame(self, style="Water.Card.TFrame")
        dates.grid(row=1, column=1, sticky="w")
        for i, (year, month) in enumerate(((self.year_start, self.month_start), (self.year_end, self.month_end))):
            if i:
                ttk.Label(dates, text=" 〜 ", style="Water.Card.TLabel").pack(side="left", padx=8)
            ttk.Entry(dates, textvariable=year, width=6).pack(side="left")
            ttk.Label(dates, text="年", style="Water.Card.TLabel").pack(side="left", padx=3)
            ttk.Combobox(dates, textvariable=month, values=[f"{n}月" for n in range(1, 13)],
                         width=5, state="readonly").pack(side="left")
        ttk.Label(self, text="時間単位", style="Water.Card.TLabel").grid(row=2, column=0, sticky="w")
        ttk.Combobox(self, textvariable=self.interval, values=("時間値", "日値"), width=10,
                     state="readonly").grid(row=2, column=1, sticky="w")
        ttk.Checkbutton(self, text="年別シートに加え、全期間シートを追加", variable=self.single_sheet,
                        style="Water.TCheckbutton").grid(row=3, column=1, sticky="w", pady=(10, 0))

    def build_request(self, export_parquet):
        return InputValidator().build_request(year_start=self.year_start.get().strip(),
            year_end=self.year_end.get().strip(), month_start=self.month_start.get(),
            month_end=self.month_end.get(), mode_type=self.mode.get(),
            use_daily=self.interval.get() == "日値", single_sheet=self.single_sheet.get(),
            export_parquet=export_parquet)


class DamConditions(ttk.Frame):
    def __init__(self, parent):
        super().__init__(parent, style="Water.Card.TFrame")
        self.start_date = tk.StringVar(self, value=f"{date.today().year}-01-01")
        self.end_date = tk.StringVar(self, value=date.today().isoformat())
        ttk.Label(self, text="時間値5項目を一括取得", style="Water.Card.TLabel").pack(anchor="w")
        ttk.Label(self, text="流域平均雨量・貯水量・流入量・放流量・貯水率",
                  style="Water.Hint.TLabel").pack(anchor="w", pady=(2, 12))
        dates = ttk.Frame(self, style="Water.Card.TFrame")
        dates.pack(anchor="w")
        for i, (label, variable) in enumerate((("開始日", self.start_date), ("終了日", self.end_date))):
            ttk.Label(dates, text=label, style="Water.Card.TLabel").grid(row=0, column=i * 2, padx=(0 if i == 0 else 18, 6))
            ttk.Entry(dates, textvariable=variable, width=13).grid(row=0, column=i * 2 + 1)
        ttk.Label(self, text="YYYY-MM-DD形式で入力（例: 2024-01-01）",
                  style="Water.Hint.TLabel").pack(anchor="w", pady=(8, 0))
        ttk.Label(self, text="31日を超える期間も指定できます。取得時に自動で分割します。",
                  style="Water.Hint.TLabel").pack(anchor="w", pady=(8, 0))

    def build_request(self, output_dir: Path, export_parquet):
        try:
            start = date.fromisoformat(self.start_date.get().strip())
            end = date.fromisoformat(self.end_date.get().strip())
        except ValueError as exc:
            raise ValueError("開始日・終了日はYYYY-MM-DD形式の実在する日付を入力してください。") from exc
        return DamRequest(start, end, output_dir, export_parquet)
