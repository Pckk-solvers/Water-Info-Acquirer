# 水文データ取得・整理支援ツールのソースコード
# 実行時にはこちらを直接実行してください。
from tkinter import Tk, Label, Button
from bs4 import BeautifulSoup
import pandas as pd
from .datemode import process_period_date_display_for_code
from .infra.http_client import HEADERS, throttled_get
from .service.flow_fetch import fetch_hourly_dataframe_for_code
from .service.flow_write import write_hourly_excel
from .ui.app import show_water as _show_water

class EmptyExcelWarning(Exception):
    """出力用データが空のときに投げる例外"""
    pass

# --- 元のデータ取得・Excel生成処理 ---
def process_data_for_code(code, Y1, Y2, M1, M2, mode_type, single_sheet=False):
    df, file_name, value_col = fetch_hourly_dataframe_for_code(
        code=code,
        year_start=Y1,
        year_end=Y2,
        month_start=M1,
        month_end=M2,
        mode_type=mode_type,
        pd=pd,
        throttled_get=throttled_get,
        headers=HEADERS,
        BeautifulSoup=BeautifulSoup,
    )
    if df is None:
        return None

    if df.empty or df[value_col].dropna().empty:
        raise EmptyExcelWarning(f"観測所コード {code}：指定期間に有効なデータが見つかりませんでした")

    write_hourly_excel(
        pd=pd,
        df=df,
        file_name=file_name,
        value_col=value_col,
        mode_type=mode_type,
        single_sheet=single_sheet,
        empty_error_type=EmptyExcelWarning,
    )

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
