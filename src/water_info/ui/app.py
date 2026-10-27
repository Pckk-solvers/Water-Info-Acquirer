"""観測所・条件・出力先・実行結果を一つの画面で扱う取得GUI。"""

import os
from pathlib import Path
import queue
import re
import tkinter as tk
from tkinter import filedialog, ttk
from tkinter.scrolledtext import ScrolledText

from water_info_acquirer.app_meta import get_module_title
from water_info_acquirer.navigation import build_navigation_menu
from ..service.acquisition import AcquisitionPlan, AcquisitionResult
from .execution import AcquisitionController
from .forms import RegularConditions, DamConditions
from .help_content import show_help
from .styles import BACKGROUND, configure_styles, card

STATUS_LABELS = {"complete": "完了", "warning": "品質注意", "partial": "一部取得",
                 "failed": "失敗", "cancelled": "中止"}


class WWRApp:
    def __init__(self, parent, fetch_hourly, fetch_daily, empty_error_type,
                 single_sheet_mode=False, on_open_other=None, on_close=None,
                 on_return_home=None, debug_ui=False, initial_codes=None):
        self.fetch_hourly = fetch_hourly
        self.fetch_daily = fetch_daily
        self.on_open_other = on_open_other
        self.on_close = on_close
        self.on_return_home = on_return_home
        self.root = tk.Toplevel(parent)
        self.root.title(get_module_title("water_info", lang="jp"))
        self.root.configure(background=BACKGROUND)
        width = min(1160, self.root.winfo_screenwidth() - 60)
        height = min(850, self.root.winfo_screenheight() - 100)
        self.root.geometry(f"{width}x{height}")
        self.root.minsize(960, 720)
        self.root.protocol("WM_DELETE_WINDOW", self._handle_close)
        configure_styles(self.root)
        self.codes = []
        self.results: dict[str, AcquisitionResult] = {}
        self._running = False
        self._controller = None
        self._poll_id = None
        self._locked = []
        self._help_window = None
        self.target = tk.StringVar(self.root, value="水位・流量・雨量")
        self.output_dir = tk.StringVar(self.root, value=str(Path("outputs/water_info").resolve()))
        self.export_parquet = tk.BooleanVar(self.root, value=False)
        self.code_input = tk.StringVar(self.root)
        self.count_text = tk.StringVar(self.root, value="0観測所")
        self.feedback = tk.StringVar(self.root)
        self.status = tk.StringVar(self.root, value="観測所と取得条件を設定してください。")
        self.progress_text = tk.StringVar(self.root, value="待機中")
        self.output_hint = tk.StringVar(self.root)
        self._build_ui(single_sheet_mode)
        for code in initial_codes or []:
            self._insert_codes(str(code))
        self._select_target()
        self.root.bind("<Configure>", self._resize_hints, add=True)
        self.root.bind("<Destroy>", self._on_destroy, add=True)

    def _build_ui(self, single_sheet):
        self.root.columnconfigure(0, weight=1)
        self.root.rowconfigure(1, weight=3)
        self.root.rowconfigure(4, weight=2, minsize=190)
        self.root.configure(menu=build_navigation_menu(self.root, current_app_key="water",
            on_open_other=self._open_other, on_return_home=self._return_home,
            extra_actions=(("この画面の使い方", self._show_help),)))
        header = ttk.Frame(self.root, padding=(22, 14), style="Water.TFrame")
        header.grid(row=0, column=0, sticky="ew")
        ttk.Label(header, text="水文データの取得", style="Water.Title.TLabel").pack(side="left")
        ttk.Button(header, text="使い方・品質の説明", command=self._show_help,
                   style="Water.TButton").pack(side="right")

        settings_view = ttk.Frame(self.root, style="Water.TFrame")
        settings_view.grid(row=1, column=0, sticky="nsew", padx=22)
        self.settings_canvas = tk.Canvas(settings_view, background=BACKGROUND, highlightthickness=0, height=400)
        settings_scroll = ttk.Scrollbar(settings_view, command=self.settings_canvas.yview)
        self.settings_canvas.configure(yscrollcommand=settings_scroll.set)
        settings_scroll.pack(side="right", fill="y")
        self.settings_canvas.pack(side="left", fill="both", expand=True)
        config = ttk.Frame(self.settings_canvas, style="Water.TFrame")
        settings_window = self.settings_canvas.create_window((0, 0), window=config, anchor="nw")
        self.settings_canvas.bind("<Configure>", lambda e: self.settings_canvas.itemconfigure(settings_window, width=e.width))
        config.bind("<Configure>", lambda _e: self.settings_canvas.configure(scrollregion=self.settings_canvas.bbox("all")))
        self.settings_canvas.bind("<MouseWheel>", self._scroll_settings)
        config.columnconfigure(1, weight=1)
        self.station_card = card(config, "1  観測所")
        self.station_card.grid(row=0, column=0, rowspan=2, sticky="nsew", padx=(0, 14))
        entry_row = ttk.Frame(self.station_card, style="Water.Card.TFrame")
        entry_row.pack(fill="x")
        self.code_entry = ttk.Entry(entry_row, textvariable=self.code_input, width=22)
        self.code_entry.pack(side="left", fill="x", expand=True)
        self.code_entry.bind("<Return>", lambda _e: self._add_code())
        ttk.Button(entry_row, text="追加", command=self._add_code, style="Water.TButton").pack(side="left", padx=(5, 0))
        ttk.Label(self.station_card, text="水文水質DBの観測所記号を入力", style="Water.Hint.TLabel").pack(anchor="w", pady=(5, 8))
        actions = ttk.Frame(self.station_card, style="Water.Card.TFrame")
        actions.pack(fill="x", pady=(0, 8))
        ttk.Button(actions, text="複数コードを貼付", command=self._paste_codes,
                   style="Water.TButton").pack(side="left")
        ttk.Button(actions, text="選択を削除", command=self._remove_code,
                   style="Water.TButton").pack(side="right", padx=(5, 0))
        listing = ttk.Frame(self.station_card, style="Water.Card.TFrame")
        listing.pack(fill="both", expand=True)
        self.station_tree = ttk.Treeview(listing, columns=("code",), show="headings", height=5,
                                        style="Water.Treeview", selectmode="extended")
        self.station_tree.heading("code", text="取得する観測所記号")
        self.station_tree.column("code", width=235, stretch=True)
        scroll = ttk.Scrollbar(listing, orient="vertical", command=self.station_tree.yview)
        self.station_tree.configure(yscrollcommand=scroll.set)
        scroll.pack(side="right", fill="y")
        self.station_tree.pack(fill="both", expand=True)
        self.station_tree.bind("<Delete>", lambda _e: self._remove_code())
        ttk.Label(self.station_card, textvariable=self.count_text, style="Water.Hint.TLabel").pack(anchor="e", pady=(6, 0))

        self.conditions_card = card(config, "2  取得条件")
        self.conditions_card.grid(row=0, column=1, sticky="nsew")
        targets = ttk.Frame(self.conditions_card, style="Water.Card.TFrame")
        targets.pack(fill="x", pady=(0, 12))
        for label in ("水位・流量・雨量", "ダム諸量"):
            ttk.Radiobutton(targets, text=label, value=label, variable=self.target,
                command=self._select_target, style="Water.TRadiobutton").pack(side="left", padx=(0, 25))
        self.regular_controls = RegularConditions(self.conditions_card, single_sheet=single_sheet)
        self.dam_panel = DamConditions(self.conditions_card)

        self.output_card = card(config, "3  出力")
        self.output_card.grid(row=1, column=1, sticky="ew", pady=(12, 0))
        folder_row = ttk.Frame(self.output_card, style="Water.Card.TFrame")
        folder_row.pack(fill="x")
        self.folder_entry = ttk.Entry(folder_row, textvariable=self.output_dir)
        self.folder_entry.pack(side="left", fill="x", expand=True)
        ttk.Button(folder_row, text="参照…", command=self._browse_output,
                   style="Water.TButton").pack(side="left", padx=(6, 0))
        ttk.Checkbutton(self.output_card, text="Parquetも追加出力する", variable=self.export_parquet,
                        style="Water.TCheckbutton").pack(anchor="w", pady=(8, 2))
        self.output_hint_label = ttk.Label(self.output_card, textvariable=self.output_hint,
                                          style="Water.Hint.TLabel", wraplength=550)
        self.output_hint_label.pack(anchor="w", fill="x")
        self.error_label = ttk.Label(self.root, textvariable=self.feedback, style="Water.Error.TLabel", wraplength=1060)
        self.error_label.grid(row=2, column=0, sticky="ew", padx=22, pady=(6, 0))

        run = ttk.Frame(self.root, padding=(22, 8), style="Water.TFrame")
        run.grid(row=3, column=0, sticky="ew")
        run.columnconfigure(0, weight=1)
        ttk.Label(run, textvariable=self.status, style="Water.TLabel").grid(row=0, column=0, sticky="w")
        self.bar = ttk.Progressbar(run, maximum=1, mode="determinate")
        self.bar.grid(row=1, column=0, sticky="ew", pady=(7, 0))
        ttk.Label(run, textvariable=self.progress_text, style="Water.TLabel").grid(row=0, column=1, padx=12)
        self.cancel_button = ttk.Button(run, text="中止", command=self._cancel, state="disabled", style="Water.TButton")
        self.cancel_button.grid(row=0, column=2, rowspan=2, padx=(0, 8))
        self.execute_button = ttk.Button(run, text="取得を開始", command=self._on_execute, style="Water.Primary.TButton")
        self.execute_button.grid(row=0, column=3, rowspan=2)

        result_card = card(self.root, "実行結果")
        result_card.grid(row=4, column=0, sticky="nsew", padx=22, pady=(0, 16))
        toolbar = ttk.Frame(result_card, style="Water.Card.TFrame")
        toolbar.pack(fill="x", pady=(0, 5))
        ttk.Label(toolbar, text="結果を選ぶと、品質やエラーの詳細を表示します。", style="Water.Hint.TLabel").pack(side="left")
        self.open_folder_button = ttk.Button(toolbar, text="保存フォルダ", command=self._open_result_folder, state="disabled", style="Water.TButton")
        self.open_folder_button.pack(side="right")
        self.open_excel_button = ttk.Button(toolbar, text="Excelを開く", command=self._open_excel, state="disabled", style="Water.TButton")
        self.open_excel_button.pack(side="right", padx=6)
        result_body = ttk.Frame(result_card, style="Water.Card.TFrame")
        result_body.pack(fill="both", expand=True)
        result_body.columnconfigure(0, weight=3)
        result_body.columnconfigure(1, weight=2)
        result_body.rowconfigure(0, weight=1)
        result_list = ttk.Frame(result_body, style="Water.Card.TFrame")
        result_list.grid(row=0, column=0, sticky="nsew", padx=(0, 10))
        self.result_tree = ttk.Treeview(result_list, columns=("station", "status", "summary"), show="headings",
                                       height=3, style="Water.Treeview", selectmode="browse")
        for col, label, width in (("station", "観測所", 200), ("status", "状態", 80), ("summary", "結果", 240)):
            self.result_tree.heading(col, text=label)
            self.result_tree.column(col, width=width, minwidth=80, stretch=col == "summary")
        scroll = ttk.Scrollbar(result_list, command=self.result_tree.yview)
        scroll.pack(side="right", fill="y")
        self.result_tree.configure(yscrollcommand=scroll.set)
        self.result_tree.pack(fill="both", expand=True)
        for tag, color in (("warning", "#8a5500"), ("partial", "#8a5500"), ("failed", "#b42318"), ("cancelled", "#526978")):
            self.result_tree.tag_configure(tag, foreground=color)
        self.result_tree.bind("<<TreeviewSelect>>", self._select_result)
        self.result_tree.bind("<Double-1>", lambda _e: self._open_excel())
        self.result_detail = ScrolledText(result_body, height=4, width=35, wrap="word", relief="flat", font=("Yu Gothic UI", 9),
                                         background="#f5f8fa", foreground="#183142", padx=8, pady=5, state="disabled")
        self.result_detail.frame.grid(row=0, column=1, sticky="nsew")
        self._bind_settings_scroll(config)

    def _bind_settings_scroll(self, parent):
        for widget in parent.winfo_children():
            if not isinstance(widget, (ttk.Treeview, ttk.Scrollbar, ttk.Combobox)):
                widget.bind("<MouseWheel>", self._scroll_settings, add=True)
            self._bind_settings_scroll(widget)

    def _scroll_settings(self, event):
        if self.settings_canvas.yview() != (0.0, 1.0) and event.delta:
            step = max(1, abs(event.delta) // 120)
            self.settings_canvas.yview_scroll(-step if event.delta > 0 else step, "units")
            return "break"

    def _resize_hints(self, event):
        if event.widget is self.root:
            self.output_hint_label.configure(wraplength=max(280, self.output_card.winfo_width() - 35))
            self.error_label.configure(wraplength=max(500, self.root.winfo_width() - 44))

    def _select_target(self, _event=None):
        self.regular_controls.pack_forget()
        self.dam_panel.pack_forget()
        if self.target.get() == "ダム諸量":
            self.dam_panel.pack(fill="x")
            self.output_hint.set("Excel1冊・年別シート / 全期間CSV・品質情報・原データ\n観測所と期間ごとのサブフォルダに保存します。")
        else:
            self.regular_controls.pack(fill="x")
            self.output_hint.set("Excelは観測所ごとに1冊・年別シート。指定したフォルダに保存します。")
        self.feedback.set("")
        self.settings_canvas.yview_moveto(0)

    def _insert_codes(self, text):
        codes = [s for s in re.split(r"[\s,，、;；]+", text.strip()) if s]
        invalid = [s for s in codes if not s.isascii() or not s.isdigit()]
        if invalid:
            self.feedback.set(f"観測所記号は半角数字で入力してください: {invalid[0]}")
            return False
        for code in codes:
            if code not in self.codes:
                self.codes.append(code)
                self.station_tree.insert("", "end", iid=code, values=(code,))
        self.count_text.set(f"{len(self.codes)}観測所")
        return True

    def _add_code(self):
        if self._running:
            return
        self.feedback.set("")
        if self._insert_codes(self.code_input.get()):
            self.code_input.set("")
        self.code_entry.focus_set()

    def _paste_codes(self):
        if self._running:
            return
        try:
            text = self.root.clipboard_get()
        except tk.TclError:
            self.feedback.set("クリップボードに観測所記号がありません。")
            return
        self.feedback.set("")
        self._insert_codes(text)

    def _remove_code(self):
        if self._running:
            return
        for code in self.station_tree.selection():
            self.codes.remove(code)
            self.station_tree.delete(code)
        self.count_text.set(f"{len(self.codes)}観測所")

    def _browse_output(self):
        folder = filedialog.askdirectory(parent=self.root, title="取得データの出力先", initialdir=self.output_dir.get())
        if folder:
            self.output_dir.set(folder)
            self.feedback.set("")

    def _build_plan(self):
        if not self.codes:
            raise ValueError("観測所記号を追加してください。")
        if not self.output_dir.get().strip():
            raise ValueError("出力フォルダを指定してください。")
        folder = Path(self.output_dir.get().strip()).expanduser().resolve()
        if folder.exists() and not folder.is_dir():
            raise ValueError("出力先にファイルが指定されています。フォルダを選択してください。")
        request = (self.dam_panel.build_request(folder, self.export_parquet.get())
            if self.target.get() == "ダム諸量" else self.regular_controls.build_request(self.export_parquet.get()))
        return AcquisitionPlan(tuple(self.codes), request, folder)

    def _set_running(self, running):
        self._running = running
        if running:
            self._locked = []

            def lock(parent):
                for widget in parent.winfo_children():
                    if isinstance(widget, (ttk.Entry, ttk.Combobox, ttk.Button, ttk.Checkbutton, ttk.Radiobutton)):
                        self._locked.append((widget, str(widget.cget("state"))))
                        widget.configure(state="disabled")
                    lock(widget)

            for frame in (self.station_card, self.conditions_card, self.output_card):
                lock(frame)
        else:
            for widget, state in self._locked:
                if widget.winfo_exists():
                    widget.configure(state=state)
            self._locked = []
        self.execute_button.configure(state="disabled" if running else "normal")
        self.cancel_button.configure(state="normal" if running else "disabled")

    def _on_execute(self):
        if self._running:
            return
        self.feedback.set("")
        try:
            plan = self._build_plan()
            plan.output_dir.mkdir(parents=True, exist_ok=True)
        except (ValueError, OSError) as exc:
            self.feedback.set(str(exc))
            return
        self.results.clear()
        self.result_tree.delete(*self.result_tree.get_children())
        self._select_result()
        self.bar.configure(value=0, maximum=1)
        self.status.set(f"{self.target.get()} / {len(plan.codes)}観測所 / 取得を開始します")
        self.progress_text.set("取得中")
        self._set_running(True)
        self._controller = AcquisitionController()
        self._controller.start(plan, fetch_hourly=self.fetch_hourly, fetch_daily=self.fetch_daily)
        self._poll_id = self.root.after(100, self._poll)

    def _poll(self):
        controller = self._controller
        if controller is None:
            return
        try:
            while True:
                kind, value = controller.events.get_nowait()
                if kind == "progress":
                    self.bar.configure(maximum=max(value.total, 1), value=value.current)
                    self.progress_text.set(f"{value.current} / {value.total}")
                    self.status.set(f"{value.code}  {value.message}")
                elif kind == "result":
                    self._add_result(value)
                elif kind == "fatal":
                    self.feedback.set(f"処理エラー: {value}")
                elif kind == "done":
                    self._set_running(False)
                    labels = [f"{label} {sum(r.status == key for r in self.results.values())}件"
                              for key, label in STATUS_LABELS.items() if any(r.status == key for r in self.results.values())]
                    self.status.set("終了: " + " / ".join(labels) if labels else "処理が終了しました。")
                    self.progress_text.set("終了")
                    self._poll_id = None
                    return
        except queue.Empty:
            pass
        self._poll_id = self.root.after(100, self._poll)

    def _cancel(self):
        if self._controller and self._running:
            self._controller.cancel()
            self.cancel_button.configure(state="disabled")
            self.status.set("中止処理中。取得済み分の保存が終わるまでお待ちください。")

    def _add_result(self, result):
        iid = result.code
        self.results[iid] = result
        self.result_tree.insert("", "end", iid=iid, values=(
            f"{result.code} {result.station_name}", STATUS_LABELS[result.status], result.summary), tags=(result.status,))
        self.result_tree.selection_set(iid)
        self.result_tree.see(iid)
        self._select_result()

    def _selected_result(self):
        selected = self.result_tree.selection()
        return self.results.get(selected[0]) if selected else None

    def _select_result(self, _event=None):
        result = self._selected_result()
        text = "" if result is None else f"{result.detail}\n保存先: {result.output_dir}"
        self.result_detail.configure(state="normal")
        self.result_detail.delete("1.0", "end")
        self.result_detail.insert("1.0", text)
        self.result_detail.configure(state="disabled")
        self.open_excel_button.configure(state="normal" if result and result.excel_path and result.excel_path.is_file() else "disabled")
        self.open_folder_button.configure(state="normal" if result and result.output_dir.is_dir() else "disabled")

    def _open_path(self, path):
        if path and path.exists():
            try:
                os.startfile(path)
            except OSError as exc:
                self.feedback.set(f"開けませんでした: {exc}")

    def _open_excel(self):
        result = self._selected_result()
        if result:
            self._open_path(result.excel_path)

    def _open_result_folder(self):
        result = self._selected_result()
        if result:
            self._open_path(result.output_dir)

    def _show_help(self):
        if self._help_window is not None and self._help_window.winfo_exists():
            self._help_window.lift()
        else:
            self._help_window = show_help(self.root)

    def _stop_polling(self):
        if self._controller:
            self._controller.cancel()
        if self._poll_id:
            self.root.after_cancel(self._poll_id)
            self._poll_id = None

    def _on_destroy(self, event):
        if event.widget is self.root:
            self._stop_polling()

    def _handle_close(self):
        self._stop_polling()
        self.root.destroy()
        if self.on_close:
            self.on_close()

    def _return_home(self):
        self._handle_transition(self.on_return_home)

    def _open_other(self, key):
        callback = self.on_open_other
        if callback:
            self._handle_transition(lambda: callback(key))

    def _handle_transition(self, callback):
        if callback:
            self._stop_polling()
            self.root.destroy()
            callback()


def show_water(parent, fetch_hourly, fetch_daily, empty_error_type, single_sheet_mode=False,
               on_open_other=None, on_close=None, on_return_home=None, debug_ui=False, initial_codes=None):
    return WWRApp(parent, fetch_hourly, fetch_daily, empty_error_type, single_sheet_mode,
                  on_open_other, on_close, on_return_home, debug_ui, initial_codes)
