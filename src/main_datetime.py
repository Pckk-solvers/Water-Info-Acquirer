# 水文データ取得・整理支援ツールのソースコード
# 実行時にはこちらを直接実行してください。
import re
import threading
import subprocess
import calendar
from datetime import datetime, timedelta

import requests
import pandas as pd
from bs4 import BeautifulSoup
import tkinter.font as tkFont
from tkinter import (
    Tk, Frame, Label, Button, Entry, Listbox,Toplevel,
    StringVar, BooleanVar, Radiobutton, Checkbutton,
    PanedWindow, ttk, LEFT, TOP, BOTTOM
)
from src.datemode import process_period_date_display_for_code

class EmptyExcelWarning(Exception):
    """出力用データが空のときに投げる例外"""
    pass

# グローバル変数・定数
width = 0
height = 0
cb_month = ['1月','2月','3月','4月','5月','6月','7月','8月','9月','10月','11月','12月']
month_dic = {'1月':0, '2月':1, '3月':2, '4月':3, '5月':4, '6月':5, '7月':6, '8月':7, '9月':8, '10月':9, '11月':10, '12月':11}

def month_floor(dt: datetime) -> datetime:
    """その月の月初(00:00)"""
    return datetime(dt.year, dt.month, 1)

def shift_month(dt: datetime, n: int) -> datetime:
    """月初を基準に n ヶ月シフトした月初"""
    y = dt.year + (dt.month - 1 + n) // 12
    m = (dt.month - 1 + n) % 12 + 1
    return datetime(y, m, 1)

# --- 元のデータ取得・Excel生成処理 ---
def process_data_for_code(code, Y1, Y2, M1, M2, mode_type, single_sheet=False):
    # --- モード別設定（ファイル名は後で生成） ---
    if mode_type == "S":
        num = "2"
        mode_str = "Water"
        elem_columns = ["水位"]
    elif mode_type == "R":
        num = "6"
        mode_str = "Water"
        elem_columns = ["流量"]
    elif mode_type == "U":
        num = "2"
        mode_str = "Rain"
        elem_columns = ["雨量"]
    else:
        return None

    # --- 月リスト・期間設定 ---
    month_list = ["0101","0201","0301","0401","0501","0601",
                  "0701","0801","0901","1001","1101","1201"]
    url_month = []
    new_month = []
    i = month_dic[M1]
    # 開始～終了までの月数を計算
    total = (month_dic[M2] - month_dic[M1] + 1) + (int(Y2) - int(Y1)) * 12
    for _ in range(total):
        new_month.append(month_list[i])
        i = (i + 1) % 12

    # 年またぎを考慮して YYYYMMDD 文字列を作成
    kariY = int(Y1)
    for m in new_month:
        url_month.append(f"{kariY}{m}")
        if m == "1201":
            kariY += 1

    # --- 観測所名取得 ---
    # 最初の URL でテーブルから観測所名をスクレイピング
    first_date = url_month[0]
    first_url = (
        f"http://www1.river.go.jp/cgi-bin/Dsp{mode_str}Data.exe"
        f"?KIND={num}&ID={code}&BGNDATE={first_date}&ENDDATE={Y2}1231&KAWABOU=NO"
    )
    res0 = requests.get(first_url)
    res0.encoding = 'euc_jp'
    soup0 = BeautifulSoup(res0.text, "html.parser")
    info_table = soup0.find_all("table", {"border":"1","cellpadding":"2","cellspacing":"1"})[0]
    data_tr = info_table.find_all("tr")[1]
    cells = data_tr.find_all("td")
    raw_name = cells[1].get_text(strip=True)               # 例: "神野瀬川（かんのせがわ）"
    station_name = re.sub(r'（.*?）', '', raw_name).strip()  # 読み仮名を除去 -> "神野瀬川"

    # --- ファイル名生成 ---
    prefix = f"{code}_{station_name}_"
    if mode_type == "S":
        file_name = f"{prefix}{Y1}年{M1}-{Y2}年{M2}_WH.xlsx"
    elif mode_type == "R":
        file_name = f"{prefix}{Y1}年{M1}-{Y2}年{M2}_QH.xlsx"
    else:  # "U"
        file_name = f"{prefix}{Y1}年{M1}-{Y2}年{M2}_RH.xlsx"

    # --- データ取得・Elemリスト構築 ---
    elem_list = []
    for um in url_month:
        url = (
            f"http://www1.river.go.jp/cgi-bin/Dsp{mode_str}Data.exe"
            f"?KIND={num}&ID={code}&BGNDATE={um}&ENDDATE={Y2}1231&KAWABOU=NO"
        )
        print(url)
        res = requests.get(url)
        res.encoding = 'euc_jp'
        soup = BeautifulSoup(res.text, "html.parser")
        elems = soup.select("td > font")
        for elem in elems:
            elem_list.extend(elem)
        # float変換できないものは空文字に
        for idx in range(len(elem_list)):
            try:
                elem_list[idx] = float(elem_list[idx])
            except:
                elem_list[idx] = ""
        if mode_type in ["S", "U"] and elem_list:
            elem_list.pop()
    if not elem_list:
        raise ValueError("指定期間のデータが取得できませんでした")


    # --- 日時インデックスの作成とDataFrame準備 ---
    year_end  = int(Y2)
    month_end = int(new_month[-1][:2])
    last_day  = calendar.monthrange(year_end, month_end)[1]
    end_date  = datetime(year_end, month_end, last_day) + timedelta(days=1)

    # 開始日時は「Y1年M1月1日 00:00」
    year_start  = int(Y1)
    month_start = int(new_month[0][:2])
    start_date  = datetime(year_start, month_start, 1, 0, 0)

    # １時間ごとの DatetimeIndex を要素数に合わせて生成
    data_date = pd.date_range(start=start_date, periods=len(elem_list), freq='h')

    # 値リストをインデックス付きで DataFrame に
    df = pd.DataFrame(elem_list, index=data_date, columns=elem_columns)

    # DatetimeIndex を列に戻す
    df = df.reset_index().rename(columns={'index': 'datetime'})
    
    # ─── 1:00 ~ 0:00 方式（表示・集計ともに＋1時間シフト）───
    # 元の datetime はそのまま保持し、display_dt を作る
    df['display_dt'] = df['datetime'] + pd.to_timedelta(1, 'h')

    # 新: 所属年は元の測定時刻ベース（表示は+1hでも“所属年”はズレない）
    df['sheet_year'] = df['datetime'].dt.year

    # 値列を数値型に変換（空文字は NaN に）
    df[elem_columns[0]] = pd.to_numeric(df[elem_columns[0]], errors='coerce')
    
    # --- 生データ DataFrame を作った直後にチェック ---
    if df.empty or df[elem_columns[0]].dropna().empty:
        # 例：EmptyExcelWarning を投げる、あるいは popup 処理へ飛ばす
        raise EmptyExcelWarning(f"観測所コード {code}：指定期間に有効なデータが見つかりませんでした")

    # --- XlsxWriter で書き出し＋チャート作成 ---
    with pd.ExcelWriter(file_name, engine='xlsxwriter',
                        datetime_format='yyyy/m/d h:mm') as writer:
        # --- フラグ: 全期間シート挿入 ---
        if single_sheet:
            # 全期間用 DataFrame を作成
            full_df = df[['display_dt'] + elem_columns].copy()
            sheet_full = "全期間"
            full_df.to_excel(
                writer,
                sheet_name=sheet_full,
                index=False
            )
            ws_full = writer.sheets[sheet_full]
            # 列幅調整
            ws_full.set_column('A:A', 20)  # datetime_dt
            ws_full.set_column('B:B', 12)  # 値

            # 全期間チャートの挿入
            chart_full = writer.book.add_chart({
                'type': 'scatter',
                'subtype': 'straight_with_markers'
            })
            max_row_full = len(full_df) + 1
            chart_full.add_series({
                'name':       sheet_full,
                'categories': [sheet_full, 1, 0, max_row_full-1, 0],
                'values':     [sheet_full, 1, 1, max_row_full-1, 1],
                'marker':     {'type': 'none'},
                'line':       {'width': 1.5},
            })
            min_dt = full_df['display_dt'].min()
            max_dt = full_df['display_dt'].max()
            # 例: "2024/6~2025/5"
            title_str = f"{min_dt.year}/{min_dt.month} - {max_dt.year}/{max_dt.month}"
            chart_full.set_title({'name': title_str})
            # Y 軸タイトル
            ytitle = {'S':'水位[m]', 'R':'流量[m^3/s]', 'U':'雨量[mm/h]'}[mode_type]
            
            min_dt = full_df['display_dt'].min()
            max_dt = full_df['display_dt'].max()

            xmin = shift_month(month_floor(min_dt), -1)  # 1ヶ月前の月初
            xmax = shift_month(month_floor(max_dt), +2)  # データの月＋1ヶ月分の“翌月初”
            
            chart_full.set_x_axis({
                'name':            '日時[月]',
                'date_axis':       True,
                'num_format':      'm',
                'major_unit':      31,
                'min':             xmin,
                'max':             xmax,
                'major_unit_type': 'months',
                'major_gridlines': {'visible': True},
                'label_position': 'low'
            })
            chart_full.set_y_axis({'name': ytitle})
            chart_full.set_legend({'position': 'none'})
            chart_full.set_size({'width': 720, 'height': 300})
            ws_full.insert_chart('D2', chart_full)

        # 年ごとにシート出力＋チャート挿入
        for year, group in df.groupby('sheet_year', sort=True):
            sheet_name = f"{year}年"
            group[['display_dt'] + elem_columns] \
                .to_excel(writer, index=False, sheet_name=sheet_name)
            ws = writer.sheets[sheet_name]

            # 列幅調整
            ws.set_column('A:A', 20)  # datetime_dt
            ws.set_column('B:B', 12)  # 値

            # チャート作成
            workbook = writer.book
            chart = workbook.add_chart({'type': 'scatter', 'subtype': 'straight_with_markers'})
            max_row = len(group) + 1
            # チャートのデータ範囲を設定
            gmin = group['display_dt'].min()
            gmax = group['display_dt'].max()
            xmin = shift_month(month_floor(gmin), -1)
            xmax = shift_month(month_floor(gmax), +2)

            chart.add_series({
                'name':       sheet_name,
                'categories': [sheet_name, 1, 0, max_row-1, 0],  # A列
                'values':     [sheet_name, 1, 1, max_row-1, 1],  # B列
                'marker':     {'type': 'none'},
                'line':       {'width': 1.5},
            })

            chart.set_x_axis({
                'name':            '日時[月]',
                'date_axis':       True,
                'num_format':      'm',
                'major_unit':      31,
                'major_unit_type': 'months',
                'min':             xmin,          # 月初に合わせる
                'max':             xmax,          # 翌年1/1まで
                'major_gridlines': {'visible': True},
                'label_position': 'low'
            })
            ytitle = {'S':'水位[m]', 'R':'流量[m^3/s]', 'U':'雨量[mm/h]'}[mode_type]
            chart.set_y_axis({'name': ytitle})
            chart.set_legend({'position': 'none'})
            chart.set_size({'width': 720, 'height': 300})

            ws.insert_chart('D2', chart)

        # --- display_time_summary シートの作成と追加 ---

        # 日別サマリ用 DataFrame を作成
        tmp = pd.DataFrame(
            [[dt.strftime('%Y/%m/%d'), val]
            for dt, val in zip(df['display_dt'], df[elem_columns[0]])],
            columns=['date', elem_columns[0]]
        )
        tmp[elem_columns[0]] = pd.to_numeric(tmp[elem_columns[0]], errors='coerce')

        daily_df = (
            tmp
              .groupby('date')
              .agg(
                  empty_count=(elem_columns[0], lambda s: s.isna().sum())
              )
              .reset_index()
        )

        # 年別サマリ用 DataFrame を作成
        year_list = []
        for year, group in df.groupby('sheet_year', sort=True):
            # ─── 非 null 値がない年はスキップ ───
            non_null = group[elem_columns[0]].dropna()
            if non_null.empty:
                continue
            max_idx    = non_null.idxmax()
            ts_max     = group.loc[max_idx, 'display_dt'].to_pydatetime()
            val_max    = group.loc[max_idx, elem_columns[0]]
            empty_year = group[elem_columns[0]].isna().sum()
            year_list.append([year, ts_max, val_max, empty_year])

        year_summary_df = pd.DataFrame(
            year_list,
            columns=['year', 'year_max_datetime', elem_columns[0], 'year_empty_count']
        )
        
        # 行はあるが全列が NaN のみ → 実質的にデータがないとみなす
        if df.empty or df.dropna(how="all").empty:
            raise EmptyExcelWarning("有効なデータがありません")


        # 同一シートに日別サマリを A/B 列へ出力
        daily_df.to_excel(
            writer,
            sheet_name='summary',
            index=False,
            startrow=0,
            startcol=0
        )
        ws = writer.sheets['summary']
        ws.set_column('A:A', 15)  # date 列
        ws.set_column('B:B', 12)  # count_empty 列

        # 同じシートに年別サマリを D〜G 列へ出力
        year_summary_df.to_excel(
            writer,
            sheet_name='summary',
            index=False,
            startrow=0,
            startcol=3
        )
        ws.set_column('D:D', 8)   # year 列
        ws.set_column('E:E', 20)  # year_max_datetime 列
        ws.set_column('F:F', 10)  # 水位などの値 列
        ws.set_column('G:G', 18)  # year_empty_count 列

    # with ブロックを抜けるとファイルが保存されます
    print(f"Excelファイルの作成が完了しました。 {file_name}")
    return file_name



def process_year_data_for_code(code, Y3, mode2):
    # ...（ここに元のprocess_year_data_for_codeの実装をそのまま貼り付け）現在廃版
    pass


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
    def __init__(self, single_sheet_mode=False):
        self.single_sheet_mode = single_sheet_mode
        self.root = Tk()
        self.root.title('水文データ取得ツール')
        self.root.config(bg="#d1f6ff")
        w, h = self.root.winfo_screenwidth(), self.root.winfo_screenheight()
        self.root.geometry(f"950x700+{(w-900)//2}+{(h-700)//2}")  # 初期サイズ＆中央
        self.root.update_idletasks()
        self.root.minsize(self.root.winfo_width(), self.root.winfo_height())



        self.codes = []
        self.mode = StringVar(value="S")
        self.use_data_sru = BooleanVar(value=False)
        self.year_start = StringVar(value=str(datetime.now().year))
        self.month_start = StringVar(value="1月")
        self.year_end = StringVar(value=str(datetime.now().year))
        self.month_end = StringVar(value="12月")
        
        # GUI 用変数に single_sheet_mode を渡す（UIで参照可能に）
        self.single_sheet_var = BooleanVar(value=self.single_sheet_mode)

        self._build_ui()
        self.root.mainloop()


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
        
        Label(main, text="※本ツールに関する問い合わせ窓口\n国土基盤事業本部 河川部 国土基盤技術研究室 南まさし", bg="#d1f6ff", font=(None, 15, 'bold')).pack(anchor='center', side=BOTTOM, pady=(5,0))

        # --- サイドパネル（Notebookタブ） ---
        notebook = ttk.Notebook(paned)
        paned.add(notebook)
        tab_side = Frame(notebook, bg="#eef6f9")
        notebook.add(tab_side, text="説明")
        self._populate_side_panel(tab_side)

    def _populate_side_panel(self, parent):
        
        # フォント定義
        title_font = tkFont.Font(family="Meiryo", size=10, weight="bold")
        desc_font = tkFont.Font(family="Meiryo", size=9)
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
            ("・指定全期間シート挿入", "指定した全期間データを1シート目へ挿入したい場合に、チェックを入れてください。", "black"),
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
        # 開始年は4桁で入力かチェック
        if not re.fullmatch(r"\d{4}", self.year_start.get()):
            self._popup('開始年は4桁で入力してください')
            return False
        # 終了年は4桁で入力かチェック
        if not re.fullmatch(r"\d{4}", self.year_end.get()):
            self._popup('終了年は4桁で入力してください')
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

        results = []
        for c in self.codes:
            try:
                # ここで EmptyExcelWarning やその他例外が飛んできます
                if self.use_data_sru.get():
                    file_path = process_period_date_display_for_code(
                        c,
                        self.year_start.get(), self.year_end.get(),
                        self.month_start.get(), self.month_end.get(),
                        self.mode.get(),
                        single_sheet=self.single_sheet_var.get()
                    )
                else:
                    file_path = process_data_for_code(
                        c,
                        self.year_start.get(), self.year_end.get(),
                        self.month_start.get(), self.month_end.get(),
                        self.mode.get(),
                        single_sheet=self.single_sheet_var.get()
                    )
                results.append(file_path)

            except EmptyExcelWarning as ee:
                # 「空データ」ケースはポップアップだけ出して次へ
                self.root.after(0, lambda msg=str(ee): self._popup(msg))
                continue  # 次のコードへ

            except Exception as e:
                # 想定外エラーもポップアップで通知して次へ
                self.root.after(
                    0,
                    lambda code=c, msg=str(e): self._popup(
                        msg
                    )
                )
                continue  # 次のコードへ

        loading.destroy()

        # 正常に出力されたファイルだけを結果表示
        if results:
            self._show_results(results)



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
            command=self.root.quit,   # or self.root.destroy
        ).pack(pady=5)



