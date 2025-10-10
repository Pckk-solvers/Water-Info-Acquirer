"""
GUI分離レイヤ

UIコンポーネントの管理、ビジネスロジックへの委譲
"""

import re
import threading
import subprocess
from datetime import datetime
from pathlib import Path
from typing import List, Tuple

import tkinter.font as tkFont
from tkinter import (
    Tk, Frame, Label, Button, Entry, Listbox, Toplevel,
    StringVar, BooleanVar, Radiobutton, Checkbutton,
    PanedWindow, ttk, LEFT, TOP, BOTTOM
)

from .api import execute_data_acquisition
from .errors import EmptyDataError, WaterInfoAcquirerError
from .constants import MODE_LABELS
from .logging_config import get_logger
from .exception_handler import create_gui_exception_handler

# ロガー取得
logger = get_logger(__name__)


def run_in_thread(func):
    """別スレッドで実行するデコレータ"""
    def wrapper(*args, **kwargs):
        threading.Thread(target=func, args=args, kwargs=kwargs, daemon=True).start()
    return wrapper


class ToolTip:
    """ツールチップクラス"""
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
        if self.tipwin: return
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
    """Water Weather Report アプリケーションクラス"""
    
    def __init__(self, single_sheet_mode: bool = False):
        """
        GUI初期化
        
        Args:
            single_sheet_mode: シングルシートモード
        """
        self.single_sheet_mode = single_sheet_mode
        self.root = Tk()
        self.root.title('水文データ取得ツール')
        self.root.config(bg="#d1f6ff")
        w, h = self.root.winfo_screenwidth(), self.root.winfo_screenheight()
        self.root.geometry(f"950x700+{(w-900)//2}+{(h-700)//2}")
        self.root.update_idletasks()
        self.root.minsize(self.root.winfo_width(), self.root.winfo_height())

        # GUI変数の初期化
        self.codes = []
        self.mode = StringVar(value="S")
        self.use_data_sru = BooleanVar(value=False)
        self.year_start = StringVar(value=str(datetime.now().year))
        self.month_start = StringVar(value="1月")
        self.year_end = StringVar(value=str(datetime.now().year))
        self.month_end = StringVar(value="12月")
        self.single_sheet_var = BooleanVar(value=self.single_sheet_mode)

        # 統一例外ハンドラーの設定
        self.exception_handler = create_gui_exception_handler(self._show_error)

        self._build_ui()
        
    def run(self):
        """GUIメインループを開始"""
        self.root.mainloop()

    def _build_ui(self):
        """UI構築"""
        # ツールタイトル
        Label(self.root,
              text="水文データ取得ツール",
              bg="#d1f6ff",
              font=(None, 24, 'bold')
              ).pack(fill='x', pady=(10,5))

        # メインとサイドを分割する PanedWindow
        paned = PanedWindow(self.root, orient='horizontal')
        paned.pack(fill='both', expand=True, padx=10, pady=10)

        # --- メイン操作領域 ---
        main = Frame(paned, bg="#d1f6ff")
        paned.add(main)
        
        # 観測所コード入力
        frame_input = Frame(main, bg="#d1f6ff")
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
        frame_list = Frame(main, width=200, height=120)
        frame_list.pack(pady=(0,5))
        frame_list.pack_propagate(False)
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
        ttk.Entry(frame_period, textvariable=self.year_start, width=6).pack(side='left')
        Label(frame_period, text="年", bg="#d1f6ff").pack(side='left')
        ttk.Combobox(frame_period, textvariable=self.month_start,
                     values=[f"{i}月" for i in range(1,13)], width=6, state="readonly").pack(side='left', padx=(2,2))
        Label(frame_period, text="～", bg="#d1f6ff").pack(side='left')
        ttk.Entry(frame_period, textvariable=self.year_end, width=6).pack(side='left')
        Label(frame_period, text="年", bg="#d1f6ff").pack(side='left')
        ttk.Combobox(frame_period, textvariable=self.month_end,
                     values=[f"{i}月" for i in range(1,13)], width=6, state="readonly").pack(side='left', padx=2)

        # 日別データ切替
        Checkbutton(main, text="日データ", variable=self.use_data_sru, bg="#d1f6ff").pack(anchor='center', pady=10)

        # 指定全期間シート挿入
        Checkbutton(main, text="指定全期間シート挿入", variable=self.single_sheet_var, bg="#d1f6ff").pack(anchor='center', pady=10)

        # 実行ボタン
        Button(main, text="実行", command=self._on_execute, height=2, width=8).pack(pady=(10,5))
        
        Label(main, text="※本ツールに関する問い合わせ窓口\n国土基盤事業本部 河川部 国土基盤技術研究室 南まさし", 
              bg="#d1f6ff", font=(None, 15, 'bold')).pack(anchor='center', side=BOTTOM, pady=(5,0))

        # --- サイドパネル（Notebookタブ） ---
        notebook = ttk.Notebook(paned)
        paned.add(notebook)
        tab_side = Frame(notebook, bg="#eef6f9")
        notebook.add(tab_side, text="説明")
        self._populate_side_panel(tab_side)

    def _populate_side_panel(self, parent):
        """サイドパネルの内容を構築"""
        self._desc_labels = []
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
             "\nまた、エラー時は「OK」ボタンを押すまで画面操作が行えなくなるため、エラーメッセージを確認後、「OK」ボタンをクリックしてください。"
             "\n[Error 13] こちらはExcelが開かれていて書き込みができない状態です。", "red")
        ]
        
        for title, text, color in sections:
            Label(parent, text=title, bg="#eef6f9", fg=color, font=(None, 10, 'bold')).pack(anchor='w', pady=(8,0), padx=5)
            lbl = Label(parent, text=text, bg="#eef6f9", fg=color, justify=LEFT, wraplength=1)
            lbl.pack(anchor='w', padx=5)
            self._desc_labels.append(lbl)

        def on_configure(event):
            new_wrap = event.width - 10
            for lbl in self._desc_labels:
                lbl.configure(wraplength=new_wrap)

        parent.bind('<Configure>', on_configure)

    def _add_code(self, entry):
        """観測所コードを追加"""
        code = entry.get().strip()
        if code.isdigit() and code not in self.codes:
            self.codes.append(code)
            self.listbox.insert('end', code)
            logger.info(f"観測所コード追加: {code}")
        entry.delete(0, 'end')

    def _remove_code(self):
        """選択された観測所コードを削除"""
        for idx in reversed(self.listbox.curselection()):
            code = self.codes[idx]
            self.listbox.delete(idx)
            self.codes.pop(idx)
            logger.info(f"観測所コード削除: {code}")

    def _validate(self):
        """入力値の検証"""
        if not self.codes:
            self._show_error('観測所コードを追加してください')
            return False
        if not re.fullmatch(r"\d{4}", self.year_start.get()):
            self._show_error('開始年は4桁で入力してください')
            return False
        if not re.fullmatch(r"\d{4}", self.year_end.get()):
            self._show_error('終了年は4桁で入力してください')
            return False
        return True

    def _show_error(self, message: str):
        """
        エラー表示の統一
        
        Args:
            message: エラーメッセージ
        """
        logger.warning(f"エラー表示: {message}")
        win = Toplevel(self.root)
        win.title('エラー')
        win.config(bg="#ffffbf")
        px = self.root.winfo_x()
        py = self.root.winfo_y()
        win.geometry(f"+{px + 200}+{py + 200}")
        Label(win, text=message, bg="#ffffbf").pack(padx=20, pady=10)
        Button(win, text="OK", command=win.destroy).pack(pady=5)
        win.transient(self.root)
        win.grab_set()
        win.wait_window()

    def _show_results(self, files: List[Path]):
        """
        結果表示
        
        Args:
            files: 生成されたファイルのリスト
        """
        logger.info(f"結果表示: {len(files)}件のファイル")
        self.root.update_idletasks()
        x = self.root.winfo_rootx()
        y = self.root.winfo_rooty()
        
        w = Toplevel(self.root)
        w.title('結果')
        w.config(bg="#d1f6ff")
        w.geometry(f"+{x}+{y}")

        Label(w, text="Excel作成完了", bg="#d1f6ff").pack(pady=10)
        for f in files:
            Label(w, text=str(f), bg="#d1f6ff").pack()

        Button(
            w,
            text="開く",
            command=lambda: [subprocess.Popen(["start", str(x)], shell=True) for x in files]
        ).pack(pady=5)

        Button(w, text="閉じる", command=w.destroy).pack(pady=5)
        Button(w, text="終了", command=self.root.quit).pack(pady=5)

    @run_in_thread
    def _on_execute(self):
        """
        実行ボタンハンドラ（ビジネスロジック呼び出し）
        """
        if not self._validate():
            return

        logger.info("データ取得処理開始")
        
        # 処理中ウィンドウ表示
        self.root.update_idletasks()
        rx = self.root.winfo_rootx()
        ry = self.root.winfo_rooty()
        rw = self.root.winfo_width()

        loading = Toplevel(self.root)
        loading.title('処理中')
        loading.config(bg="#d1f6ff")
        loading.geometry(f"+{rx + rw + 10}+{ry}")
        Label(loading, text="処理中...", bg="#d1f6ff").pack(padx=20, pady=20)
        loading.update()

        try:
            # 月文字列を数値に変換
            month_dic = {'1月':1, '2月':2, '3月':3, '4月':4, '5月':5, '6月':6, 
                         '7月':7, '8月':8, '9月':9, '10月':10, '11月':11, '12月':12}
            
            start_month = month_dic[self.month_start.get()]
            end_month = month_dic[self.month_end.get()]
            granularity = "day" if self.use_data_sru.get() else "hour"
            
            # 統合APIを呼び出し（統一例外ハンドラー付き）
            results, errors = execute_data_acquisition(
                codes=self.codes,
                start_year=int(self.year_start.get()),
                start_month=start_month,
                end_year=int(self.year_end.get()),
                end_month=end_month,
                mode=self.mode.get(),
                granularity=granularity,
                single_sheet=self.single_sheet_var.get(),
                exception_handler=self.exception_handler
            )
            
            loading.destroy()
            
            # エラーがあった場合は個別に表示
            for code, error_msg in errors:
                self.root.after(0, lambda msg=error_msg: self._show_error(msg))
            
            # 成功したファイルがあれば結果表示
            if results:
                self.root.after(0, lambda files=results: self._show_results(files))
            elif not errors:
                # 結果もエラーもない場合（通常は発生しない）
                self.root.after(0, lambda: self._show_error("処理が完了しましたが、結果がありません"))
                
        except Exception as e:
            loading.destroy()
            # 統一例外ハンドリング
            context = "GUI実行処理"
            self.exception_handler.handle_exception(e, context)