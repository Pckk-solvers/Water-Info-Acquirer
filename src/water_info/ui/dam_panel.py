"""従来の取得画面に置くダム専用パネル。"""

from datetime import date
import os
from pathlib import Path
import queue
import threading
import tkinter as tk
from tkinter import filedialog, ttk

from ..domain.dam import DAM_METRICS, DamRequest
from ..infra.dam_writer import quality_summary, write_dam_outputs
from ..service.dam import fetch_dam_data


class DamPanel(ttk.Frame):
    def __init__(self, parent, *, get_codes):
        super().__init__(parent)
        self.get_codes = get_codes
        self.start_date = tk.StringVar(self, value=f"{date.today().year}-01-01")
        self.end_date = tk.StringVar(self, value=date.today().isoformat())
        self.output_dir = tk.StringVar(self, value=str(Path("outputs/water_info/dam").resolve()))
        self.parquet = tk.BooleanVar(self, value=True)
        self.status = tk.StringVar(self, value="5項目の時間値をまとめて取得します。")
        self._running = False
        self._stop = threading.Event()
        self._queue = queue.Queue()
        self._poll_id = None
        ttk.Label(self, text="ダム諸量（時間値）", font=("", 13, "bold")).pack(pady=6)
        ttk.Label(self, text="流域平均雨量・貯水量・流入量・放流量・貯水率").pack()
        dates = ttk.Frame(self)
        dates.pack(pady=8)
        for label, variable in (("開始日", self.start_date), ("終了日", self.end_date)):
            ttk.Label(dates, text=label).pack(side="left", padx=4)
            ttk.Entry(dates, textvariable=variable, width=12).pack(side="left")
        ttk.Label(self, text="YYYY-MM-DD形式。元日付の24時まで取得／長期間は自動分割").pack()
        folder = ttk.Frame(self)
        folder.pack(pady=8, fill="x")
        ttk.Label(folder, text="出力先").pack(side="left")
        ttk.Entry(folder, textvariable=self.output_dir, width=35).pack(side="left", fill="x", expand=True)
        ttk.Button(folder, text="参照", command=self._browse).pack(side="left")
        ttk.Checkbutton(self, text="Parquetも出力（Excel・CSVは常に出力）", variable=self.parquet).pack()
        buttons = ttk.Frame(self)
        buttons.pack(pady=8)
        self.execute_button = ttk.Button(buttons, text="ダム諸量を取得", command=self.start)
        self.execute_button.pack(side="left", padx=4)
        self.cancel_button = ttk.Button(buttons, text="中止", command=self._cancel, state="disabled")
        self.cancel_button.pack(side="left")
        ttk.Button(buttons, text="出力先を開く", command=self._open_output).pack(side="left", padx=4)
        self.bar = ttk.Progressbar(self, mode="determinate")
        self.bar.pack(fill="x", padx=8)
        ttk.Label(self, textvariable=self.status, wraplength=460).pack(pady=4)
        self.result_text = tk.Text(self, height=7, width=56, wrap="word", state="disabled")
        self.result_text.pack(fill="both", expand=True, padx=4, pady=4)
        self.bind("<Destroy>", self._on_destroy, add=True)

    def _on_destroy(self, event):
        if event.widget is self:
            self._stop.set()
            if self._poll_id:
                self.after_cancel(self._poll_id)

    def _browse(self):
        folder = filedialog.askdirectory(parent=self, title="ダム諸量の出力先")
        if folder:
            self.output_dir.set(folder)

    def _open_output(self):
        path = Path(self.output_dir.get()).expanduser()
        if path.is_dir():
            os.startfile(path)

    def _cancel(self):
        self._stop.set()
        self.cancel_button.configure(state="disabled")
        self.status.set("中止処理中。取得済み分と未取得区間を保存します。")

    def start(self):
        if self._running:
            return
        try:
            codes = list(self.get_codes())
            if not codes or any(not c.isascii() or not c.isdigit() for c in codes):
                raise ValueError("観測所記号を上の一覧に追加してください")
            if not self.output_dir.get().strip():
                raise ValueError("出力先を指定してください")
            request = DamRequest(date.fromisoformat(self.start_date.get().strip()),
                date.fromisoformat(self.end_date.get().strip()),
                Path(self.output_dir.get()).expanduser().resolve(), self.parquet.get())
        except ValueError as exc:
            self.status.set(f"入力エラー: {exc}")
            return
        self._running = True
        self._stop.clear()
        self.execute_button.configure(state="disabled")
        self.cancel_button.configure(state="normal")
        self.result_text.configure(state="normal")
        self.result_text.delete("1.0", "end")
        self.result_text.configure(state="disabled")
        self.status.set("取得を開始します。")
        self.bar["value"] = 0
        threading.Thread(target=self._work, args=(codes, request), daemon=True).start()
        self._poll_id = self.after(100, self._poll)

    def _work(self, codes, request):
        try:
            for code in codes:
                if self._stop.is_set():
                    break
                sub = request.output_dir / f"{code}_{request.start:%Y%m%d}_{request.end:%Y%m%d}"
                actual = DamRequest(request.start, request.end, sub, request.export_parquet)
                try:
                    data = fetch_dam_data(code, actual, should_stop=self._stop.is_set,
                        progress=lambda n, total, r: self._queue.put(("progress", (n, total,
                            f"{code}: {r['start']}〜{r['end']} {r['status']}"))))
                    write_dam_outputs(data, progress=lambda message: self._queue.put(("status", message)))
                    quality = quality_summary(data.frame)
                    state = {"complete": "全区間取得完了", "partial": "一部取得失敗",
                             "cancelled": "中止・部分保存"}[data.status]
                    details = [f"{code} {state}／{len(data.frame):,}行"]
                    for m in DAM_METRICS:
                        count = quality[m.key]["normal"]
                        details.append(f"{m.label}: 有効 {count:,} / 欠測等 {len(data.frame) - count:,}")
                    details.append(f"出力先: {sub}")
                    self._queue.put(("result", "\n".join(details)))
                except Exception as exc:
                    self._queue.put(("result", f"{code}: 処理失敗 {exc}"))
        finally:
            self._queue.put(("done", None))

    def _poll(self):
        self._poll_id = None
        if not self.winfo_exists():
            return
        try:
            while True:
                kind, value = self._queue.get_nowait()
                if kind == "progress":
                    n, total, message = value
                    self.bar.configure(maximum=total, value=n)
                    self.status.set(message)
                elif kind == "status":
                    self.status.set(value)
                elif kind == "result":
                    self.result_text.configure(state="normal")
                    self.result_text.insert("end", value + "\n\n")
                    self.result_text.see("end")
                    self.result_text.configure(state="disabled")
                elif kind == "done":
                    self._running = False
                    self.execute_button.configure(state="normal")
                    self.cancel_button.configure(state="disabled")
                    self.status.set("処理が終了しました。取得状態と品質は結果を確認してください。")
                    return
        except queue.Empty:
            pass
        self._poll_id = self.after(100, self._poll)
