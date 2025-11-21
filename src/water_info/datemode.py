import calendar
import re
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Optional, TYPE_CHECKING

pd = None
BeautifulSoup = None
requests = None
req_exc = None

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

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/129.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Connection": "close",
    # 必要なら:
    # "Referer": "http://www1.river.go.jp/",
    # "Upgrade-Insecure-Requests": "1",
}

REQUEST_MIN_DELAY = 1.0
REQUEST_DELAY_STEP = 0.2
REQUEST_MAX_DELAY = 2.0
REQUEST_MAX_RETRIES = 5
REQUEST_BACKOFF_CAP = 10
RETRYABLE_STATUS = {429, 500, 502, 503, 504}

_REQUEST_LOCK = threading.Lock()
_REQUEST_COUNTER = 0


def _calc_delay(request_index: int) -> float:
    if request_index <= 0:
        return 0.0
    delay = REQUEST_MIN_DELAY + REQUEST_DELAY_STEP * (request_index - 1)
    return min(delay, REQUEST_MAX_DELAY)


def throttled_get(url: str, headers: dict, timeout: int = 30) -> "Response":
    """
    リクエスト間隔を最低限確保しつつ、一時的な失敗時には再試行を行うGETラッパー。
    """
    _ensure_http_client()
    global _REQUEST_COUNTER
    last_error: Optional[Exception] = None
    for attempt in range(1, REQUEST_MAX_RETRIES + 1):
        with _REQUEST_LOCK:
            current_index = _REQUEST_COUNTER
            _REQUEST_COUNTER += 1
        delay = _calc_delay(current_index)
        if delay:
            time.sleep(delay)

        try:
            response = requests.get(url, headers=headers, timeout=timeout)
        except req_exc.RequestException as exc:
            last_error = exc
            if attempt == REQUEST_MAX_RETRIES:
                break
            backoff = min(REQUEST_MIN_DELAY * (2 ** (attempt - 1)), REQUEST_BACKOFF_CAP)
            time.sleep(backoff)
            continue

        if response.status_code in RETRYABLE_STATUS and attempt < REQUEST_MAX_RETRIES:
            last_error = req_exc.HTTPError(
                f"HTTP {response.status_code} while requesting {url}"
            )
            backoff = min(REQUEST_MIN_DELAY * (2 ** (attempt - 1)), REQUEST_BACKOFF_CAP)
            time.sleep(backoff)
            continue

        response.raise_for_status()
        return response

    if last_error:
        raise last_error
    raise RuntimeError(f"{url} の取得に失敗しました")

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
    if mode_type == "S":
        num = "3"
        data_label = "水位"
        chart_title = "水位[m]"
        file_suffix = "_WD.xlsx"
        base_url = "http://www1.river.go.jp/cgi-bin/DspWaterData.exe?"
    elif mode_type == "R":
        num = "7"
        data_label = "流量"
        chart_title = "流量[m^3/s]"
        file_suffix = "_QD.xlsx"
        base_url = "http://www1.river.go.jp/cgi-bin/DspWaterData.exe?"
    elif mode_type == "U":
        num = "3"
        data_label = "雨量"
        chart_title = "雨量[mm/h]"
        file_suffix = "_RD.xlsx"
        base_url = "http://www1.river.go.jp/cgi-bin/DspRainData.exe?"
    else:
        print("mode_typeは 'S', 'R', または 'U' を指定してください。")
        return None
    
    # --- 観測所名取得 ---
    first_url = (
        f"{base_url}KIND={num}&ID={code}&BGNDATE={Y1}0101&ENDDATE={Y1}1231&KAWABOU=NO"
    )
    # コンソールにURLをプリント
    debug_tag = f"[WWR][daily][mode={mode_type}]"
    print(
        f"{debug_tag} 初回取得 -> 観測所コード={code}, 対象年={Y1}-{Y2}, "
        f"月範囲={M1}-{M2}, url={first_url}"
    )
    res0 = throttled_get(first_url, headers=HEADERS)
    res0.encoding = 'euc_jp'
    soup0 = BeautifulSoup(res0.text, "html.parser")
    info_table = soup0.find_all("table", {"border":"1","cellpadding":"2","cellspacing":"1"})[0]
    data_tr = info_table.find_all("tr")[1]
    cells = data_tr.find_all("td")
    raw_name = cells[1].get_text(strip=True)
    station_name = re.sub(r'（.*?）', '', raw_name).strip()

    # ファイル名生成
    out_dir = Path("water_info")                                                                                                                                                                                                                                                                               
    out_dir.mkdir(parents=True, exist_ok=True)                                                                                                                                                                                                                                                                 
    file_name = out_dir / f"out{code}_{station_name}_{Y1}年{M1}-{Y2}年{M2}{file_suffix}"
    print(f"生成ファイル名: {file_name}")

    # --- 年単位でデータ取得・日付インデックス化 ---
    years = list(range(int(Y1), int(Y2) + 1))
    all_values, all_dates = [], []
    for year in years:
        url = (
            f"{base_url}KIND={num}&ID={code}&BGNDATE={year}0101&ENDDATE={year}1231&KAWABOU=NO"
        )
        # コンソールにURLをプリント
        print(f"{debug_tag} 年度取得 -> year={year}, url={url}")
        res = throttled_get(url, headers=HEADERS)
        res.encoding = 'euc_jp'
        vals = [f.get_text() for f in BeautifulSoup(res.text, "html.parser").select("td > font")]
        vals = pd.to_numeric(pd.Series(vals), errors="coerce").tolist()
        last = calendar.monthrange(year, 12)[1]
        dates = pd.date_range(start=f"{year}-01-01", end=f"{year}-12-{last}", freq="D")
        n = min(len(dates), len(vals))
        all_dates += list(dates[:n])
        all_values += vals[:n]

    # DataFrame 作成・期間フィルタリング
    df = pd.DataFrame({data_label: all_values}, index=all_dates)
    start_dt = datetime(int(Y1), int(M1.replace("月","")), 1)
    end_dt = datetime(int(Y2), int(M2.replace("月","")), calendar.monthrange(int(Y2), int(M2.replace("月","")))[1])
    df = df[(df.index >= start_dt) & (df.index <= end_dt)]
    df.sort_index(inplace=True)
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
            full_df.to_excel(
                writer,
                sheet_name=sheet_full,
                index=False,
                columns=['datetime', data_label]
            )
            ws_full = writer.sheets[sheet_full]
            # 列幅調整
            ws_full.set_column("A:A", 15)  # datetime
            ws_full.set_column("B:B", 12)  # data_label
            #散布図挿入
            chart = wb.add_chart({"type":"scatter","subtype":"straight_with_markers"})
            max_row = len(full_df) + 1
            chart.add_series({
                "name": sheet_full,
                "categories": [sheet_full, 1, 0, max_row-1, 0],
                "values":     [sheet_full, 1, 1, max_row-1, 1],
                "marker":     {"type":"none"},
                "line":       {"width":1.5},
            })
            chart.set_x_axis({"name":"日時[年/月]","date_axis":True,"num_format":"yyyy/mm","major_unit":185,"major_gridlines":{"visible":True}})
            chart.set_y_axis({"name":chart_title})
            chart.set_legend({"position":"none"})
            chart.set_size({"width":720,"height":300})
            chart.set_title({"name": f"{Y1}/{M1} - {Y2}/{M2}"})
            ws_full.insert_chart("D6", chart)
        
        # --- 年別シート挿入 ---
        for year, grp in df.groupby(df.index.year):
            sheet = f"{year}年"
            grp_df = grp.reset_index().rename(columns={"index":"datetime"})
            grp_df.to_excel(writer, sheet_name=sheet, index=False, columns=["datetime", data_label])
            ws = writer.sheets[sheet]
            ws.set_column("A:A", 15)
            ws.set_column("B:B", 12)

            vals = grp_df[data_label]
            valid = vals.dropna()
            if not valid.empty:
                sheet_max_val = valid.max()
                sheet_max_date = grp_df.loc[valid.idxmax(), "datetime"].strftime("%Y/%m/%d")
                sheet_min_val = valid.min()
                sheet_min_date = grp_df.loc[valid.idxmin(), "datetime"].strftime("%Y/%m/%d")
                sheet_avg_val = valid.mean()
            else:
                sheet_max_date = sheet_min_date = sheet_avg_val = ""
                sheet_max_val = sheet_min_val = ""
            sheet_empty = int(vals.isna().sum())

            # シート集計情報
            ws.write("D1", "シート最大値発生日")
            ws.write("E1", sheet_max_date)
            ws.write("F1", sheet_max_val)
            ws.write("D2", "シート最小値発生日")
            ws.write("E2", sheet_min_date)
            ws.write("F2", sheet_min_val)
            ws.write("D3", "シート平均値")
            ws.write("E3", sheet_avg_val)
            ws.write("D4", "シート空データ数")
            ws.write("E4", sheet_empty)
            ws.set_column("D:D", 20)
            ws.set_column("E:E", 12)
            ws.set_column("F:F", 12)

            # チャート挿入
            chart = wb.add_chart({"type":"scatter","subtype":"straight_with_markers"})
            max_row = len(grp_df) + 1
            chart.add_series({
                "name": sheet,
                "categories": [sheet, 1, 0, max_row-1, 0],
                "values":     [sheet, 1, 1, max_row-1, 1],
                "marker":     {"type":"none"},
                "line":       {"width":1.5},
            })
            chart.set_x_axis({"name":"日時[月]","date_axis":True,"num_format":"mm","major_unit":30,"major_gridlines":{"visible":True}})
            chart.set_y_axis({"name":chart_title})
            chart.set_legend({"position":"none"})
            chart.set_size({"width":720,"height":300})
            ws.insert_chart("D6", chart)

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

def _ensure_http_client() -> None:
    """requests 関連の import も初回利用時に遅延させる。"""
    global requests, req_exc
    if requests is None or req_exc is None:
        import requests as requests_module
        from requests import exceptions as exceptions_module

        requests = requests_module
        req_exc = exceptions_module
