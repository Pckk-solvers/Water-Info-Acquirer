"""Tkinter UI for water_info."""

from __future__ import annotations

import threading
import time
import subprocess
from datetime import datetime

from tkinter import (
    Frame, Label, Button, Entry, Listbox, Toplevel, Menu, Tk,
    StringVar, BooleanVar, Radiobutton, Checkbutton,
    PanedWindow, ttk, LEFT, TOP, BOTTOM
)

from src.app_names import get_module_title
from ..domain.models import Options, Period, WaterInfoRequest
from ..service.usecase import fetch_water_info


def run_in_thread(func):
    """別スレッドで実行するデコレータ"""
    def wrapper(*args, **kwargs):
        threading.Thread(target=func, args=args, kwargs=kwargs, daemon=True).start()
    return wrapper


class ToolTip:
    def __init__(self, widget, text, delay=500):
        self.widget = widget
        self.text = text
        self.delay = delay
        self.tipwin = None
        self.id = None
        widget.bind('<Enter>', self.schedule)
        widget.bind('<Leave>', self.hide)

    def schedule(self, event=None):
        self.id = self.widget.after(self.delay, self.show, event)

    def show(self, event):
        if self.tipwin:
            return
        x = event.x_root + 10
        y = event.y_root + 10
        self.tipwin = tw = Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")
        lbl = Label(tw, text=self.text, background="#ffffe0",
                    relief='solid', borderwidth=1, font=("Arial", 9))
        lbl.pack()

    def hide(self, event=None):
        if self.id:
            self.widget.after_cancel(self.id)
            self.id = None
        if self.tipwin:
            self.tipwin.destroy()
            self.tipwin = None


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
    ):
        self.fetch_hourly = fetch_hourly
        self.fetch_daily = fetch_daily
        self.empty_error_type = empty_error_type
        self.single_sheet_mode = single_sheet_mode
        self.parent = parent
        self.on_open_other = on_open_other
        self.on_close = on_close
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

        self._last_validation_msg = None
        self._last_validation_at = 0.0
        self._validation_cooldown_sec = 1.0
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

    def _clear_placeholder(self, entry, placeholder):
        if entry.get() == placeholder:
            entry.delete(0, 'end')
            entry.config(fg='black')

    def _add_placeholder(self, entry, placeholder):
        if not entry.get():
            entry.insert(0, placeholder)
            entry.config(fg='grey')

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
        Button(main, text="実行", command=self._on_execute, height=2, width=8).pack(pady=(10,5))

        Label(main, text="※本ツールに関する問い合わせ窓口\n国土基盤事業本部 流域計画部 技術研究室 南まさし", bg="#d1f6ff", font=(None, 15, 'bold')).pack(anchor='center', side=BOTTOM, pady=(5,0))

        # --- サイドパネル（Notebookタブ） ---
        notebook = ttk.Notebook(paned)
        paned.add(notebook)
        tab_side = Frame(notebook, bg="#eef6f9")
        notebook.add(tab_side, text="説明")
        self._populate_side_panel(tab_side)
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

    def _populate_side_panel(self, parent):

        self._desc_labels = []  # 後で wraplength を更新するために保持
        sections = [
            ("- ツールの説明 -", "本ツールは「国土交通省・水文水質データベース」で公開されている水位・流量・雨量データを取得するツールです。"
                                "取得したデータはExcel形式（1観測所1ファイル）で出力されます。"
                                "複数年分のデータをまとめて取得した際は、取得した年数分のシートが作成されます。", "black"),
            ("・観測所記号入力欄", "半角数字でコードを入力し、[追加]をクリックしてください。", "black"),
            ("・「追加／削除」", "追加ボタンをクリック、または「Enterキー」を押すと、「観測所記号入力欄」に入力した観測所記号が「データ取得観測所一覧」へ追加されます。"
                                "「データ取得観測所一覧」から観測所を選択し削除ボタンをクリックすると、「データ取得観測所一覧」から選択した観測所を削除することができます。", "black"),
            ("・データ取得観測所一覧", "ここに表示されている観測所のデータが取得されます。", "black"),
            ("・取得項目", "水位・流量・雨量の中から、データを取得したい項目を選択してください。\n※1項目のみ選択可能", "black"),
            ("・取得期間", "データを取得したい期間の開始年月と終了年月を入力してください。", "black"),
            ("・日データ", "時刻データではなく、日データを取得したい場合に、チェックを入れてください。", "black"),
            ("・指定全期間シート挿入", "年ごとのシートに加えて、全期間のデータを1シートにまとめたものを作成したい場合に、チェックを入れてください。", "black"),
            ("・注意事項", "指定した取得期間内に有効なデータが1件も存在しない場合は、下記エラーメッセージが表示され、該当観測所のExcelファイルは出力されません。"
             "\n「指定期間に有効なデータが見つかりませんでした。」"
             "\nまた、エラー時は”OK”ボタンを押すまで画面操作が行えなくなるため、エラーメッセージを確認後、”OK”ボタンをクリックしてください。"
             "\n[Error 13] こちらはExcelが開かれていて書き込みができない状態です。", "red")
        ]
        for title, text, color in sections:
            Label(parent, text=title, bg="#eef6f9", fg=color, font=(None, 10, 'bold')).pack(anchor='w', pady=(8,0), padx=5)
            lbl = Label(parent, text=text, bg="#eef6f9", fg=color, justify=LEFT, wraplength=1)
            lbl.pack(anchor='w', padx=5)
            self._desc_labels.append(lbl)  # ラベルをリストに追加

        # フレームサイズが変わるたびに wraplength をフレーム幅に合わせて更新
        def on_configure(event):
            new_wrap = event.width - 10  # パディング分を差し引く
            for lbl in self._desc_labels:
                lbl.configure(wraplength=new_wrap)

        parent.bind('<Configure>', on_configure)

    def _add_code(self, entry):
        code = entry.get().strip()
        if code.isdigit() and code not in self.codes:
            self.codes.append(code)
            self.listbox.insert('end', code)
        entry.delete(0, 'end')

    def _remove_code(self):
        for idx in reversed(self.listbox.curselection()):
            self.listbox.delete(idx)
            self.codes.pop(idx)

    def _validate(self):
        # 観測所コードが未入力の場合
        if not self.codes:
            self._popup('観測所コードを追加してください')
            return False
        return True

    def _popup(self, msg):
        # Toplevel にして親ウィンドウに紐づけ
        win = Toplevel(self.root)
        win.title('エラー')
        win.config(bg="#ffffbf")
        # メインウィンドウの座標取得
        px = self.root.winfo_x()
        py = self.root.winfo_y()
        # メインウィンドウの右下に 20px ずらして表示
        win.geometry(f"+{px + 200}+{py + 200}")
        Label(win, text=msg, bg="#ffffbf").pack(padx=20, pady=10)
        Button(win, text="OK", command=win.destroy).pack(pady=5)
        win.transient(self.root)   # 親ウィンドウの上に出す
        win.grab_set()             # フォーカスを奪う
        win.wait_window()          # このウィンドウが閉じられるまで次の処理を待つ

    def _can_validate_inputs(self) -> bool:
        if not self.year_start.get() or not self.year_end.get():
            return False
        if len(self.year_start.get()) != 4 or len(self.year_end.get()) != 4:
            return False
        if not self.month_start.get() or not self.month_end.get():
            return False
        return True

    def _on_validate_inputs(self, _event=None):
        if not self._can_validate_inputs():
            self._set_period_error("")
            return
        try:
            WaterInfoRequest(
                period=Period(
                    year_start=self.year_start.get(),
                    year_end=self.year_end.get(),
                    month_start=self.month_start.get(),
                    month_end=self.month_end.get(),
                ),
                mode_type=self.mode.get(),
                options=Options(
                    use_daily=self.use_data_sru.get(),
                    single_sheet=self.single_sheet_var.get(),
                ),
            )
            self._set_period_error("")
        except ValueError as exc:
            msg = f"入力エラー: {exc}"
            now = time.monotonic()
            if self._last_validation_msg == msg and (now - self._last_validation_at) < self._validation_cooldown_sec:
                return
            self._last_validation_msg = msg
            self._last_validation_at = now
            self._set_period_error(msg)

    def _set_period_error(self, msg: str) -> None:
        self.period_error.configure(text=msg)

    def _clear_period_error_if_same(self, msg: str) -> None:
        if self.period_error.cget("text") == msg:
            self.period_error.configure(text="")

    def _clear_period_error_on_change(self, _event=None) -> None:
        if self.period_error.cget("text"):
            self.period_error.configure(text="")

    @run_in_thread
    def _on_execute(self):
        if not self._validate():
            return

        # メインウィンドウの座標・サイズを確定
        self.root.update_idletasks()
        rx = self.root.winfo_rootx()
        ry = self.root.winfo_rooty()
        rw = self.root.winfo_width()

        # 処理中ウィンドウをToplevelで作成し、右隣に配置
        loading = Toplevel(self.root)
        loading.title('処理中')
        loading.config(bg="#d1f6ff")
        loading.geometry(f"+{rx + rw + 10}+{ry}")
        Label(loading, text="処理中...", bg="#d1f6ff").pack(padx=20, pady=20)
        loading.update()

        try:
            request = WaterInfoRequest(
                period=Period(
                    year_start=self.year_start.get(),
                    year_end=self.year_end.get(),
                    month_start=self.month_start.get(),
                    month_end=self.month_end.get(),
                ),
                mode_type=self.mode.get(),
                options=Options(
                    use_daily=self.use_data_sru.get(),
                    single_sheet=self.single_sheet_var.get(),
                ),
            )
        except ValueError as exc:
            self._set_period_error(f"入力エラー: {exc}")
            loading.destroy()
            return
        results, errors = fetch_water_info(
            codes=self.codes,
            request=request,
            fetch_hourly=self.fetch_hourly,
            fetch_daily=self.fetch_daily,
        )

        for err in errors:
            if isinstance(err.error, self.empty_error_type):
                self.root.after(0, lambda msg=err.message: self._popup(msg))
            else:
                self.root.after(0, lambda code=err.code, msg=err.message: self._popup(f"観測所コード {code}：{msg}"))

        loading.destroy()

        if results:
            self._show_results([r.file_path for r in results])

    def _show_results(self, files):
        # メインウィンドウの座標を取得
        self.root.update_idletasks()             # レイアウトを確定
        x = self.root.winfo_rootx()              # スクリーン上の X 座標
        y = self.root.winfo_rooty()              # スクリーン上の Y 座標

        # 結果ウィンドウはルートとは別のToplevelで作成
        w = Toplevel(self.root)
        w.title('結果')
        w.config(bg="#d1f6ff")
        w.geometry(f"+{x}+{y}")                # サイズ指定をせず位置だけ指定

        Label(w, text="Excel作成完了", bg="#d1f6ff").pack(pady=10)
        for f in files:
            Label(w, text=f, bg="#d1f6ff").pack()

        Button(
            w,
            text="開く",
            command=lambda: [subprocess.Popen(["start", x], shell=True) for x in files]
        ).pack(pady=5)

        # このウィンドウだけ閉じるボタン
        Button(
            w,
            text="閉じる",
            command=w.destroy
        ).pack(pady=5)

        # アプリケーション全体を終了する“終了”ボタン
        Button(
            w,
            text="終了",
            command=self._handle_close,   # or self.root.destroy
        ).pack(pady=5)


def show_error(message: str):
    """
    予期せぬエラーをダイアログで表示
    """
    win = Tk()
    win.title("想定外エラー")
    win.config(bg="#ff7755")
    for text in [
        "想定外のエラーが発生した可能性があります", message,
        "一度全て閉じてから再試行してください",
        "問い合わせ窓口に相談してください"
    ]:
        Label(win, text=text, bg="#ff7755").pack(padx=10, pady=5)
    Button(win, text="終了", command=win.destroy).pack(pady=10)
    win.mainloop()


def show_water(
    parent,
    fetch_hourly,
    fetch_daily,
    empty_error_type,
    single_sheet_mode=False,
    on_open_other=None,
    on_close=None,
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
    )
