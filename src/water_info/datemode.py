from bs4 import BeautifulSoup
import pandas as pd

class EmptyExcelWarning(Exception):
    """出力用データが空のときに投げる例外"""
    pass

from .infra.http_client import HEADERS, throttled_get
from .service.flow_fetch import fetch_daily_dataframe_for_code
from .service.flow_write import write_daily_excel

def process_period_date_display_for_code(code, Y1, Y2, M1, M2, mode_type, single_sheet=False):
    """
    年単位URL（各年のBGNDATE=YYYY0101, ENDDATE=YYYY1231）を用いて指定年分のデータを取得し、
    開始月・終了月で指定された期間（例：2022/1～2023/9）にフィルタリング後、
    各シートにデータテーブル（A～C列）および追加統計情報（シート別・全体統計）を
    列D～Eに配置し、更に散布図をセル"D7"に配置するExcelファイルを出力します。

    追加：観測所コードに対応する観測所名をスクレイピングで取得し、ファイル名に挿入
    """
    df, file_name, data_label, chart_title = fetch_daily_dataframe_for_code(
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
        print("mode_typeは 'S', 'R', または 'U' を指定してください。")
        return None

    if df.empty or df[data_label].dropna().empty:
        raise EmptyExcelWarning(f"観測所コード {code}：指定期間に有効なデータが見つかりませんでした")

    write_daily_excel(
        pd=pd,
        df=df,
        file_name=file_name,
        data_label=data_label,
        chart_title=chart_title,
        single_sheet=single_sheet,
    )

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
