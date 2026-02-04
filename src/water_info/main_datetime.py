# 水文データ取得・整理支援ツールのソースコード
# 実行時にはこちらを直接実行してください。
import calendar
from datetime import datetime, timedelta
from pathlib import Path

from tkinter import Tk, Label, Button
from .datemode import process_period_date_display_for_code
from .infra.http_client import HEADERS, throttled_get
from .infra.url_builder import build_hourly_base, build_hourly_url
from .infra.excel_writer import add_scatter_chart, write_table, set_column_widths
from .infra.dataframe_utils import build_hourly_dataframe
from .infra.excel_summary import build_daily_empty_summary, build_year_summary
from .infra.fetching import fetch_station_name, fetch_hourly_values
from .ui.app import show_water as _show_water

pd = None
BeautifulSoup = None


def _ensure_data_libs() -> None:
    """GUI起動を早くするため、重いライブラリは初回使用時に読み込む。"""
    global pd, BeautifulSoup
    if pd is None:
        import pandas as pandas_module

        pd = pandas_module
    if BeautifulSoup is None:
        from bs4 import BeautifulSoup as bs_class

        BeautifulSoup = bs_class

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
    _ensure_data_libs()
    # --- モード別設定（ファイル名は後で生成） ---
    if mode_type == "S":
        elem_columns = ["水位"]
    elif mode_type == "R":
        elem_columns = ["流量"]
    elif mode_type == "U":
        elem_columns = ["雨量"]
    else:
        return None

    num, mode_str = build_hourly_base(mode_type)

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
    first_url = build_hourly_url(code, num, mode_str, first_date, f"{Y2}1231")
    debug_tag = f"[WWR][hourly][mode={mode_type}]"
    print(
        f"{debug_tag} 初回取得 -> 観測所コード={code}, "
        f"期間={Y1}{M1}-{Y2}{M2}, 開始月={first_date}, url={first_url}"
    )
    station_name = fetch_station_name(throttled_get, HEADERS, BeautifulSoup, first_url)

    # --- ファイル名生成 ---
    out_dir = Path("water_info")                                                                                                                                                                                                                                                                               
    out_dir.mkdir(parents=True, exist_ok=True)                                                                                                                                                                                                                                                                 
    prefix = out_dir / f"{code}_{station_name}_"  
    if mode_type == "S":
        file_name = f"{prefix}{Y1}年{M1}-{Y2}年{M2}_WH.xlsx"
    elif mode_type == "R":
        file_name = f"{prefix}{Y1}年{M1}-{Y2}年{M2}_QH.xlsx"
    else:  # "U"
        file_name = f"{prefix}{Y1}年{M1}-{Y2}年{M2}_RH.xlsx"

    # --- データ取得・Elemリスト構築 ---
    url_list = []
    for um in url_month:
        url = build_hourly_url(code, num, mode_str, um, f"{Y2}1231")
        print(f"{debug_tag} 月次取得 -> 開始={um}, 終了={Y2}1231, url={url}")
        url_list.append(url)
    elem_list = fetch_hourly_values(
        throttled_get,
        HEADERS,
        BeautifulSoup,
        url_list,
        drop_last=mode_type in ["S", "U"],
    )
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

    df = build_hourly_dataframe(pd, elem_list, start_date, elem_columns[0])
    
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
            ws_full = write_table(
                writer,
                sheet_full,
                full_df,
                column_widths={"A:A": 20, "B:B": 12},
            )

            # 全期間チャートの挿入
            max_row_full = len(full_df) + 1
            min_dt = full_df['display_dt'].min()
            max_dt = full_df['display_dt'].max()
            # 例: "2024/6~2025/5"
            title_str = f"{min_dt.year}/{min_dt.month} - {max_dt.year}/{max_dt.month}"
            # Y 軸タイトル
            ytitle = {'S':'水位[m]', 'R':'流量[m^3/s]', 'U':'雨量[mm/h]'}[mode_type]
            
            min_dt = full_df['display_dt'].min()
            max_dt = full_df['display_dt'].max()

            xmin = shift_month(month_floor(min_dt), -1)  # 1ヶ月前の月初
            xmax = shift_month(month_floor(max_dt), +2)  # データの月＋1ヶ月分の“翌月初”
            
            add_scatter_chart(
                worksheet=ws_full,
                workbook=writer.book,
                sheet_name=sheet_full,
                max_row=max_row_full,
                x_col=0,
                y_col=1,
                name=sheet_full,
                insert_cell="D2",
                x_axis={
                    'name':            '日時[月]',
                    'date_axis':       True,
                    'num_format':      'm',
                    'major_unit':      31,
                    'min':             xmin,
                    'max':             xmax,
                    'major_unit_type': 'months',
                    'major_gridlines': {'visible': True},
                    'label_position': 'low'
                },
                y_axis={'name': ytitle},
                title=title_str,
            )

        # 年ごとにシート出力＋チャート挿入
        for year, group in df.groupby('sheet_year', sort=True):
            sheet_name = f"{year}年"
            ws = write_table(
                writer,
                sheet_name,
                group[['display_dt'] + elem_columns],
                column_widths={"A:A": 20, "B:B": 12},
            )
            max_row = len(group) + 1
            # チャートのデータ範囲を設定
            gmin = group['display_dt'].min()
            gmax = group['display_dt'].max()
            xmin = shift_month(month_floor(gmin), -1)
            xmax = shift_month(month_floor(gmax), +2)

            ytitle = {'S':'水位[m]', 'R':'流量[m^3/s]', 'U':'雨量[mm/h]'}[mode_type]
            add_scatter_chart(
                worksheet=ws,
                workbook=writer.book,
                sheet_name=sheet_name,
                max_row=max_row,
                x_col=0,
                y_col=1,
                name=sheet_name,
                insert_cell="D2",
                x_axis={
                    'name':            '日時[月]',
                    'date_axis':       True,
                    'num_format':      'm',
                    'major_unit':      31,
                    'major_unit_type': 'months',
                    'min':             xmin,
                    'max':             xmax,
                    'major_gridlines': {'visible': True},
                    'label_position': 'low'
                },
                y_axis={'name': ytitle},
            )

        # --- display_time_summary シートの作成と追加 ---

        daily_df = build_daily_empty_summary(pd, df, elem_columns[0])
        year_summary_df = build_year_summary(pd, df, elem_columns[0])
        
        # 行はあるが全列が NaN のみ → 実質的にデータがないとみなす
        if df.empty or df.dropna(how="all").empty:
            raise EmptyExcelWarning("有効なデータがありません")


        # 同一シートに日別サマリを A/B 列へ出力
        ws = write_table(
            writer,
            "summary",
            daily_df,
            column_widths={"A:A": 15, "B:B": 12},
            startrow=0,
            startcol=0,
        )

        # 同じシートに年別サマリを D〜G 列へ出力
        write_table(
            writer,
            "summary",
            year_summary_df,
            startrow=0,
            startcol=3,
        )
        set_column_widths(ws, {"D:D": 8, "E:E": 20, "F:F": 10, "G:G": 18})

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


def show_water(parent, single_sheet_mode=False, on_open_other=None, on_close=None):
    """Factory for launcher to create water_info window."""
    return _show_water(
        parent=parent,
        fetch_hourly=process_data_for_code,
        fetch_daily=process_period_date_display_for_code,
        empty_error_type=EmptyExcelWarning,
        single_sheet_mode=single_sheet_mode,
        on_open_other=on_open_other,
        on_close=on_close,
    )
