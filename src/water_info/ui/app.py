"""Tkinter UI for water_info."""

from __future__ import annotations

from datetime import datetime

from tkinter import (
    Frame, Label, Button, Entry, Listbox, Toplevel, Menu,
    StringVar, BooleanVar, Radiobutton, Checkbutton,
    PanedWindow, ttk, TOP, BOTTOM
)

from src.app_names import get_module_title
from ..domain.models import WaterInfoRequest
from .execution import ExecutionController, estimate_unit_total, to_snapshot
from .debug import log
from .validation import InputValidator, format_input_error_message
from .progress_window import ProgressWindow
from .dialogs import show_error_popup, show_results
from .side_panel import populate_side_panel


class WWRApp:
    def __init__(
        self,
        parent,
        fetch_hourly,
        fetch_daily,
        empty_error_type,
        single_sheet_mode=False,
        on_open_other=None,
        on_close=None,
        debug_ui: bool = False,
        initial_codes: list[str] | None = None,
    ):
        self.fetch_hourly = fetch_hourly
        self.fetch_daily = fetch_daily
        self.empty_error_type = empty_error_type
        self.single_sheet_mode = single_sheet_mode
        self.parent = parent
        self.on_open_other = on_open_other
        self.on_close = on_close
        self._debug_ui = debug_ui
        self._initial_codes = initial_codes or []
        self.root = Toplevel(parent)
        # 親を非表示にしていても子が前面に来るように設定
        self.root.title(get_module_title("water_info", lang="jp"))
        self.root.config(bg="#d1f6ff")
        w, h = self.root.winfo_screenwidth(), self.root.winfo_screenheight()
        self.root.geometry(f"950x750+{(w-900)//2}+{(h-700)//2}")  # 初期サイズを中央に
        self.root.update_idletasks()
        self.root.minsize(self.root.winfo_width(), self.root.winfo_height())
        self.root.protocol("WM_DELETE_WINDOW", self._handle_close)
        self.root.deiconify()
        self.root.lift()
        self.root.focus_force()

        self.codes = []
        self.mode = StringVar(value="S")
        self.use_data_sru = BooleanVar(value=False)
        self.year_start = StringVar(value=str(datetime.now().year))
        self.month_start = StringVar(value="1月")
        self.year_end = StringVar(value=str(datetime.now().year))
        self.month_end = StringVar(value="12月")

        # GUI 用変数。single_sheet_mode をUIに反映
        self.single_sheet_var = BooleanVar(value=self.single_sheet_mode)

        self._validator = InputValidator(cooldown_sec=1.0)
        self._clear_error_on_change = True
        self._build_ui()

    def _open_jma(self):
        if self.on_open_other:
            self.root.destroy()
            self.on_open_other('jma')

    def _handle_close(self):
        try:
            self.root.destroy()
        finally:
            if self.on_close:
                self.on_close()

    def _build_ui(self):
        # ツールタイトル
        Label(self.root,
              text=get_module_title("water_info", lang="jp"),
              bg="#d1f6ff",
              font=(None, 24, 'bold')
              ).pack(fill='x', pady=(10,5))

        menubar = Menu(self.root)
        nav_menu = Menu(menubar, tearoff=0)
        nav_menu.add_command(label=get_module_title("jma", lang="jp"), command=self._open_jma)
        menubar.add_cascade(label="メニュー", menu=nav_menu)
        self.root.config(menu=menubar)

        # メインとサイドを分割する PanedWindow
        paned = PanedWindow(self.root, orient='horizontal')
        paned.pack(fill='both', expand=True, padx=10, pady=10)

        # --- メイン操作領域 ---
        main = Frame(paned, bg="#d1f6ff")
        paned.add(main)
        # 観測所コード入力
        # 例：コード追加／削除ボタンをまとめたフレーム
        frame_input = Frame(main, bg="#d1f6ff")
        # → 中央寄せするには fill を外し、anchor='center' を指定
        frame_input.pack(pady=5, anchor='center')
        Label(frame_input, text="観測所記号入力欄                      ", bg="#d1f6ff").pack(anchor='center')
        entry = Entry(frame_input, textvariable=StringVar(), width=20)
        entry.pack(side='left', padx=5)
        entry.bind('<Return>', lambda ev: self._add_code(entry))
        btn_add = Button(frame_input, text="追加", command=lambda: self._add_code(entry))
        btn_add.pack(side='left', padx=(2,0))
        btn_del = Button(frame_input, text="削除", command=self._remove_code)
        btn_del.pack(side='left')

        # リスト表示
        Label(main, text="データ取得観測所一覧", bg="#d1f6ff").pack(anchor='center', pady=(10,0))
        # 固定サイズコンテナを作成
        frame_list = Frame(main, width=200, height=120)  # 幅200px、高さ120px
        frame_list.pack(pady=(0,5))
        frame_list.pack_propagate(False)                # 中のwidgetでFrameサイズを変えない

        # Frame 内に Listbox を配置
        self.listbox = Listbox(frame_list)
        self.listbox.pack(fill='both', expand=True)
        self._load_initial_codes()

        # 取得項目選択
        frame_item = Frame(main, bg="#d1f6ff")
        frame_item.pack(pady=7, anchor='center')
        Label(frame_item, text="取得項目", bg="#d1f6ff").pack(side=TOP)
        for txt, val in [('水位','S'), ('流量','R'), ('雨量','U')]:
            Radiobutton(frame_item, text=txt, variable=self.mode, value=val,
                        indicatoron=False, bg="#d1f6ff").pack(side='left', padx=(2))

        # 期間指定
        frame_period = Frame(main, bg="#d1f6ff")
        frame_period.pack(anchor='center', pady=7)
        Label(frame_period, text="取得期間", bg="#d1f6ff").pack(side=TOP)
        self.period_error = Label(frame_period, text="", bg="#d1f6ff", fg="#c62828", font=(None, 9, "bold"))
        self.period_error.pack(side=TOP)
        self.entry_year_start = ttk.Entry(frame_period, textvariable=self.year_start, width=6)
        self.entry_year_start.pack(side='left')
        Label(frame_period, text="年", bg="#d1f6ff").pack(side='left')
        self.combo_month_start = ttk.Combobox(
            frame_period,
            textvariable=self.month_start,
            values=[f"{i}月" for i in range(1,13)],
            width=6,
            state="readonly",
        )
        self.combo_month_start.pack(side='left', padx=(2,2))
        Label(frame_period, text="～", bg="#d1f6ff").pack(side='left')
        self.entry_year_end = ttk.Entry(frame_period, textvariable=self.year_end, width=6)
        self.entry_year_end.pack(side='left')
        Label(frame_period, text="年", bg="#d1f6ff").pack(side='left')
        self.combo_month_end = ttk.Combobox(
            frame_period,
            textvariable=self.month_end,
            values=[f"{i}月" for i in range(1,13)],
            width=6,
            state="readonly",
        )
        self.combo_month_end.pack(side='left', padx=2)

        # 日別データ切替
        Checkbutton(main, text="日データ", variable=self.use_data_sru, bg="#d1f6ff").pack(anchor='center', pady=10)

        # 指定全期間シート挿入
        Checkbutton(main, text="指定全期間シート挿入", variable=self.single_sheet_var, bg="#d1f6ff").pack(anchor='center', pady=10)

        # 実行ボタン
        self.execute_button = Button(main, text="実行", command=self._on_execute, height=2, width=8)
        self.execute_button.pack(pady=(10,5))

        Label(main, text="※本ツールに関する問い合わせ窓口\n国土基盤事業本部 流域計画部 技術研究室 南まさし", bg="#d1f6ff", font=(None, 15, 'bold')).pack(anchor='center', side=BOTTOM, pady=(5,0))

        # --- サイドパネル（Notebookタブ） ---
        notebook = ttk.Notebook(paned)
        paned.add(notebook)
        tab_side = Frame(notebook, bg="#eef6f9")
        notebook.add(tab_side, text="説明")
        populate_side_panel(tab_side)
        self._bind_validation_events()

    def _bind_validation_events(self):
        self.entry_year_start.bind("<FocusOut>", self._on_validate_inputs)
        self.entry_year_end.bind("<FocusOut>", self._on_validate_inputs)
        self.entry_year_start.bind("<KeyRelease>", self._on_validate_inputs)
        self.entry_year_end.bind("<KeyRelease>", self._on_validate_inputs)
        self.combo_month_start.bind("<<ComboboxSelected>>", self._on_validate_inputs)
        self.combo_month_end.bind("<<ComboboxSelected>>", self._on_validate_inputs)

        if self._clear_error_on_change:
            self.entry_year_start.bind("<KeyRelease>", self._clear_period_error_on_change)
            self.entry_year_end.bind("<KeyRelease>", self._clear_period_error_on_change)
            self.combo_month_start.bind("<<ComboboxSelected>>", self._clear_period_error_on_change)
            self.combo_month_end.bind("<<ComboboxSelected>>", self._clear_period_error_on_change)

    def _add_code(self, entry):
        code = entry.get().strip()
        if code.isdigit() and code not in self.codes:
            self.codes.append(code)
            self.listbox.insert('end', code)
        entry.delete(0, 'end')

    def _load_initial_codes(self) -> None:
        for code in self._initial_codes:
            code = str(code).strip()
            if code.isdigit() and code not in self.codes:
                self.codes.append(code)
                self.listbox.insert('end', code)

    def _remove_code(self):
        for idx in reversed(self.listbox.curselection()):
            self.listbox.delete(idx)
            self.codes.pop(idx)

    def _validate(self):
        # 観測所コードが未入力の場合
        if not self.codes:
            show_error_popup(self.root, "入力エラー: 観測所コード未入力")
            return False
        return True

    def _on_validate_inputs(self, _event=None):
        if not self._validator.can_validate(
            self.year_start.get(),
            self.year_end.get(),
            self.month_start.get(),
            self.month_end.get(),
        ):
            self._set_period_error("")
            return
        try:
            self._validator.build_request(
                year_start=self.year_start.get(),
                year_end=self.year_end.get(),
                month_start=self.month_start.get(),
                month_end=self.month_end.get(),
                mode_type=self.mode.get(),
                use_daily=self.use_data_sru.get(),
                single_sheet=self.single_sheet_var.get(),
            )
            self._set_period_error("")
        except ValueError as exc:
            msg = format_input_error_message(exc)
            if self._validator.should_throttle(msg):
                return
            self._set_period_error(msg)

    def _set_period_error(self, msg: str) -> None:
        self.period_error.configure(text=msg)

    def _clear_period_error_on_change(self, _event=None) -> None:
        if self.period_error.cget("text"):
            self.period_error.configure(text="")

    def _on_execute(self):
        if not self._validate():
            return
        log(self._debug_ui, "[UI] execute start")
        self._set_execute_enabled(False)

        # メインウィンドウの座標・サイズを確定
        self.root.update_idletasks()
        rx = self.root.winfo_rootx()
        ry = self.root.winfo_rooty()
        rw = self.root.winfo_width()

        request = self._build_request()
        if request is None:
            log(self._debug_ui, "[UI] request build failed")
            self._set_execute_enabled(True)
            return
        self._start_execution(request, rx + rw + 10, ry)

    def _show_results(self, files):
        show_results(self.root, files, on_exit=self._handle_close)

    def _build_request(self) -> WaterInfoRequest | None:
        try:
            return self._validator.build_request(
                year_start=self.year_start.get(),
                year_end=self.year_end.get(),
                month_start=self.month_start.get(),
                month_end=self.month_end.get(),
                mode_type=self.mode.get(),
                use_daily=self.use_data_sru.get(),
                single_sheet=self.single_sheet_var.get(),
            )
        except ValueError as exc:
            self._set_period_error(format_input_error_message(exc))
            return None

    def _finish_processing(self, progress_window: ProgressWindow, results) -> None:
        progress_window.destroy()
        self._set_execute_enabled(True)
        if results:
            self._show_results([r.file_path for r in results])

    def _start_execution(self, request: WaterInfoRequest, progress_x: int, progress_y: int) -> None:
        unit_total = estimate_unit_total(self.codes, request)
        total_hint = unit_total or len(self.codes)
        progress_window = ProgressWindow(self.root, progress_x, progress_y, total_hint)
        started_at = ProgressWindow.now()
        log(self._debug_ui, f"[UI] progress window created total_hint={total_hint}")

        controller = ExecutionController()
        ui_queue = controller.start(
            codes=self.codes,
            request=request,
            fetch_hourly=self.fetch_hourly,
            fetch_daily=self.fetch_daily,
            unit_total=unit_total,
        )
        log(self._debug_ui, "[UI] worker started, begin polling")

        def _schedule_next():
            self.root.after(100, _poll)

        def _poll():
            log(self._debug_ui, "[UI] poll tick")
            if not progress_window.exists():
                return
            controller.poll_queue(
                ui_queue,
                on_progress=lambda progress: self._on_progress(progress, progress_window, started_at),
                on_error=self._on_error,
                on_done=lambda results: self._on_done(results, progress_window),
                schedule_next=_schedule_next,
            )

        _poll()

    def _on_progress(self, progress, progress_window: ProgressWindow, started_at: float) -> None:
        if progress is None:
            return
        if not progress_window.exists():
            return
        log(
            self._debug_ui,
            "[UI] progress",
            f"processed={progress.processed}",
            f"total={progress.total}",
            f"success={progress.success}",
            f"failed={progress.failed}",
            f"code={progress.current_code}",
            f"station={progress.current_station}",
        )
        snapshot = to_snapshot(progress, ProgressWindow.now() - started_at)
        progress_window.update(snapshot)

    def _on_error(self, err) -> None:
        log(
            self._debug_ui,
            "[UI] error",
            f"code={err.code}",
            f"type={err.error_type}",
            f"msg={err.message}",
        )
        if isinstance(err.error, self.empty_error_type):
            show_error_popup(self.root, f"データ未取得: {err.message}")
        else:
            show_error_popup(self.root, f"処理エラー: 観測所コード {err.code} {err.message}")

    def _on_done(self, results, progress_window: ProgressWindow) -> None:
        log(self._debug_ui, f"[UI] done results={len(results)}")
        self._finish_processing(progress_window, results)

    def _set_execute_enabled(self, enabled: bool) -> None:
        if hasattr(self, "execute_button") and self.execute_button.winfo_exists():
            self.execute_button.config(state="normal" if enabled else "disabled")


def show_water(
    parent,
    fetch_hourly,
    fetch_daily,
    empty_error_type,
    single_sheet_mode=False,
    on_open_other=None,
    on_close=None,
    debug_ui: bool = False,
    initial_codes: list[str] | None = None,
):
    """Factory for launcher to create water_info window."""
    return WWRApp(
        parent=parent,
        fetch_hourly=fetch_hourly,
        fetch_daily=fetch_daily,
        empty_error_type=empty_error_type,
        single_sheet_mode=single_sheet_mode,
        on_open_other=on_open_other,
        on_close=on_close,
        debug_ui=debug_ui,
        initial_codes=initial_codes,
    )
