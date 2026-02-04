import calendar
from datetime import datetime
from pathlib import Path
from typing import Optional, TYPE_CHECKING

pd = None
BeautifulSoup = None

if TYPE_CHECKING:
    from requests import Response


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

from .infra.http_client import HEADERS, throttled_get
from .infra.url_builder import build_daily_base, build_daily_base_url, build_daily_url
from .infra.excel_writer import add_scatter_chart, write_table, set_column_widths
from .infra.dataframe_utils import build_daily_dataframe
from .infra.excel_summary import build_sheet_stats
from .infra.fetching import fetch_station_name, fetch_daily_values

def process_period_date_display_for_code(code, Y1, Y2, M1, M2, mode_type, single_sheet=False):
    _ensure_data_libs()
    """
    年単位URL（各年のBGNDATE=YYYY0101, ENDDATE=YYYY1231）を用いて指定年分のデータを取得し、
    開始月・終了月で指定された期間（例：2022/1～2023/9）にフィルタリング後、
    各シートにデータテーブル（A～C列）および追加統計情報（シート別・全体統計）を
    列D～Eに配置し、更に散布図をセル"D7"に配置するExcelファイルを出力します。

    追加：観測所コードに対応する観測所名をスクレイピングで取得し、ファイル名に挿入
    """
    # モード別設定
    try:
        num, data_label, chart_title, file_suffix = build_daily_base(mode_type)
        base_url = build_daily_base_url(mode_type)
    except ValueError:
        print("mode_typeは 'S', 'R', または 'U' を指定してください。")
        return None
    
    # --- 観測所名取得 ---
    first_url = build_daily_url(base_url, code, num, f"{Y1}0101", f"{Y1}1231")
    # コンソールにURLをプリント
    debug_tag = f"[WWR][daily][mode={mode_type}]"
    print(
        f"{debug_tag} 初回取得 -> 観測所コード={code}, 対象年={Y1}-{Y2}, "
        f"月範囲={M1}-{M2}, url={first_url}"
    )
    station_name = fetch_station_name(throttled_get, HEADERS, BeautifulSoup, first_url)

    # ファイル名生成
    out_dir = Path("water_info")                                                                                                                                                                                                                                                                               
    out_dir.mkdir(parents=True, exist_ok=True)                                                                                                                                                                                                                                                                 
    file_name = out_dir / f"{code}_{station_name}_{Y1}年{M1}-{Y2}年{M2}{file_suffix}"
    print(f"生成ファイル名: {file_name}")

    # --- 年単位でデータ取得・日付インデックス化 ---
    years = list(range(int(Y1), int(Y2) + 1))
    all_values, all_dates = [], []
    for year in years:
        url = build_daily_url(base_url, code, num, f"{year}0101", f"{year}1231")
        # コンソールにURLをプリント
        print(f"{debug_tag} 年度取得 -> year={year}, url={url}")
        vals = fetch_daily_values(throttled_get, HEADERS, BeautifulSoup, pd, url)
        last = calendar.monthrange(year, 12)[1]
        dates = pd.date_range(start=f"{year}-01-01", end=f"{year}-12-{last}", freq="D")
        n = min(len(dates), len(vals))
        all_dates += list(dates[:n])
        all_values += vals[:n]

    # DataFrame 作成・期間フィルタリング
    start_dt = datetime(int(Y1), int(M1.replace("月","")), 1)
    end_dt = datetime(int(Y2), int(M2.replace("月","")), calendar.monthrange(int(Y2), int(M2.replace("月","")))[1])
    df = build_daily_dataframe(pd, all_values, all_dates, data_label, start_dt, end_dt)
    # --- 生データ空チェックをここに挿入 ---
    if df.empty or df[data_label].dropna().empty:
        raise EmptyExcelWarning(f"観測所コード {code}：指定期間に有効なデータが見つかりませんでした")



    # --- XlsxWriter で書き出し＋シート内集計＋チャート ---
    with pd.ExcelWriter(file_name, engine="xlsxwriter", datetime_format="yyyy/mm/dd") as writer:
        wb = writer.book
        # --- フラグ: 全期間シート挿入 ---
        if single_sheet:
            # 全期間用 DataFrame を作成
            full_df = df.reset_index().rename(columns={'index': 'datetime'})
            sheet_full = "全期間"
            # 日付とデータ列のみを出力
            ws_full = write_table(
                writer,
                sheet_full,
                full_df,
                column_widths={"A:A": 15, "B:B": 12},
                columns=["datetime", data_label],
            )
            max_row = len(full_df) + 1
            add_scatter_chart(
                worksheet=ws_full,
                workbook=wb,
                sheet_name=sheet_full,
                max_row=max_row,
                x_col=0,
                y_col=1,
                name=sheet_full,
                insert_cell="D6",
                x_axis={
                    "name": "日時[年/月]",
                    "date_axis": True,
                    "num_format": "yyyy/mm",
                    "major_unit": 185,
                    "major_gridlines": {"visible": True},
                },
                y_axis={"name": chart_title},
                title=f"{Y1}/{M1} - {Y2}/{M2}",
            )
        
        # --- 年別シート挿入 ---
        for year, grp in df.groupby(df.index.year):
            sheet = f"{year}年"
            grp_df = grp.reset_index().rename(columns={"index":"datetime"})
            ws = write_table(
                writer,
                sheet,
                grp_df,
                column_widths={"A:A": 15, "B:B": 12},
                columns=["datetime", data_label],
            )

            stats = build_sheet_stats(grp_df, data_label)

            # シート集計情報
            ws.write("D1", "シート最大値発生日")
            ws.write("E1", stats["sheet_max_date"])
            ws.write("F1", stats["sheet_max_val"])
            ws.write("D2", "シート最小値発生日")
            ws.write("E2", stats["sheet_min_date"])
            ws.write("F2", stats["sheet_min_val"])
            ws.write("D3", "シート平均値")
            ws.write("E3", stats["sheet_avg_val"])
            ws.write("D4", "シート空データ数")
            ws.write("E4", stats["sheet_empty"])
            set_column_widths(ws, {"D:D": 20, "E:E": 12, "F:F": 12})

            # チャート挿入
            max_row = len(grp_df) + 1
            add_scatter_chart(
                worksheet=ws,
                workbook=wb,
                sheet_name=sheet,
                max_row=max_row,
                x_col=0,
                y_col=1,
                name=sheet,
                insert_cell="D6",
                x_axis={
                    "name": "日時[月]",
                    "date_axis": True,
                    "num_format": "mm",
                    "major_unit": 30,
                    "major_gridlines": {"visible": True},
                },
                y_axis={"name": chart_title},
            )

    # 完了メッセージ
    print(f"生成完了: {file_name}")
    return file_name


if __name__ == "__main__":
    test_code_S_R = "307051287711170"
    test_code_U = "102111282214010"
    test_Y1 = "2022"
    test_Y2 = "2023"
    test_M1 = "1月"
    test_M2 = "9月"
    print("テスト開始: 期間指定モードのdate_display（水位）")
    print(process_period_date_display_for_code(test_code_S_R, test_Y1, test_Y2, test_M1, test_M2, mode_type="S"))
    print("\nテスト開始: 期間指定モードのdate_display（流量）")
    print(process_period_date_display_for_code(test_code_S_R, test_Y1, test_Y2, test_M1, test_M2, mode_type="R"))
    print("\nテスト開始: 期間指定モードのdate_display（雨量）")
    print(process_period_date_display_for_code(test_code_U, test_Y1, test_Y2, test_M1, test_M2, mode_type="U"))
