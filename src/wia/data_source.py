"""
データ取得の統合レイヤ

観測所情報取得、URL生成、時系列データ取得の共通ロジックを提供
"""

import re
import requests
import pandas as pd
from bs4 import BeautifulSoup
from typing import Optional

from .types import DataRequest, StationInfo
from .constants import MODE_CONFIG, BASE_URL, ENCODING
from .logging_config import get_logger, log_function_call, log_function_result, log_error_with_context
from .errors import NetworkError, ParseError

# ロガー取得
logger = get_logger(__name__)


def generate_url(code: str, mode: str, start_date: str, end_date: str, granularity: str) -> str:
    """
    統一されたURL生成
    
    Args:
        code: 観測所コード
        mode: モード（S/R/U）
        start_date: 開始日（YYYYMMDD形式）
        end_date: 終了日（YYYYMMDD形式）
        granularity: 粒度（hour/day）
    
    Returns:
        生成されたURL
    """
    log_function_call(logger, "generate_url", 
                     code=code, mode=mode, start_date=start_date, 
                     end_date=end_date, granularity=granularity)
    
    try:
        config = MODE_CONFIG[mode][granularity]
        num = config["num"]
        base_url = config["base_url"]
        
        url = (
            f"{BASE_URL}{base_url}"
            f"?KIND={num}&ID={code}&BGNDATE={start_date}&ENDDATE={end_date}&KAWABOU=NO"
        )
        
        log_function_result(logger, "generate_url", f"URL生成完了: {url}")
        return url
        
    except KeyError as e:
        error_msg = f"無効なモードまたは粒度: mode={mode}, granularity={granularity}"
        logger.error(error_msg)
        raise ValueError(error_msg) from e


def fetch_station_info(code: str, mode: str) -> StationInfo:
    """
    観測所情報を取得
    
    Args:
        code: 観測所コード
        mode: モード（S/R/U）
    
    Returns:
        観測所情報
    """
    log_function_call(logger, "fetch_station_info", code=code, mode=mode)
    
    try:
        # 最初の年のデータを使って観測所名を取得
        url = generate_url(code, mode, "20230101", "20231231", "day")
        logger.info(f"観測所情報取得URL: {url}")
        
        response = requests.get(url, timeout=30)
        response.encoding = ENCODING
        
        if response.status_code != 200:
            raise NetworkError(f"HTTP {response.status_code}: 観測所情報の取得に失敗しました")
        
        soup = BeautifulSoup(response.text, "html.parser")
        
        # 観測所名抽出の共通ロジック（get_text(strip=True)基準）
        info_tables = soup.find_all("table", {"border": "1", "cellpadding": "2", "cellspacing": "1"})
        if not info_tables:
            raise ParseError("観測所情報テーブルが見つかりません")
            
        info_table = info_tables[0]
        data_trs = info_table.find_all("tr")
        if len(data_trs) < 2:
            raise ParseError("観測所情報の行が不足しています")
            
        data_tr = data_trs[1]
        cells = data_tr.find_all("td")
        if len(cells) < 2:
            raise ParseError("観測所情報のセルが不足しています")
            
        raw_name = cells[1].get_text(strip=True)
        
        # 読み仮名を除去して観測所名を取得
        station_name = re.sub(r'（.*?）', '', raw_name).strip()
        
        station_info = StationInfo(
            code=code,
            name=station_name,
            raw_name=raw_name
        )
        
        log_function_result(logger, "fetch_station_info", 
                          f"観測所情報取得完了: {station_name}")
        return station_info
        
    except requests.RequestException as e:
        log_error_with_context(logger, e, f"観測所情報取得のネットワークエラー (code={code})")
        raise NetworkError(f"観測所情報の取得でネットワークエラーが発生しました: {str(e)}") from e
    except (AttributeError, IndexError) as e:
        log_error_with_context(logger, e, f"観測所情報の解析エラー (code={code})")
        raise ParseError(f"観測所情報の解析に失敗しました: {str(e)}") from e


def _extract_values_from_html(html_content: str) -> list:
    """
    HTMLから数値データを抽出
    
    Args:
        html_content: HTML文字列
    
    Returns:
        数値リスト
    """
    soup = BeautifulSoup(html_content, "html.parser")
    elems = soup.select("td > font")
    
    values = []
    for elem in elems:
        text = elem.get_text(strip=True)
        values.append(text)
    
    # pd.to_numeric(errors="coerce")による数値化を標準化
    numeric_values = pd.to_numeric(pd.Series(values), errors="coerce").tolist()
    
    return numeric_values


def _fetch_data_for_period(code: str, mode: str, start_date: str, end_date: str, granularity: str) -> list:
    """
    指定期間のデータを取得
    
    Args:
        code: 観測所コード
        mode: モード（S/R/U）
        start_date: 開始日（YYYYMMDD形式）
        end_date: 終了日（YYYYMMDD形式）
        granularity: 粒度（hour/day）
    
    Returns:
        数値リスト
    """
    try:
        url = generate_url(code, mode, start_date, end_date, granularity)
        logger.info(f"データ取得URL: {url}")
        
        response = requests.get(url, timeout=30)
        response.encoding = ENCODING
        
        if response.status_code != 200:
            raise NetworkError(f"HTTP {response.status_code}: データ取得に失敗しました")
        
        values = _extract_values_from_html(response.text)
        logger.debug(f"期間データ取得完了: {len(values)}件 ({start_date}-{end_date})")
        
        return values
        
    except requests.RequestException as e:
        log_error_with_context(logger, e, 
                             f"期間データ取得のネットワークエラー (code={code}, {start_date}-{end_date})")
        raise NetworkError(f"データ取得でネットワークエラーが発生しました: {str(e)}") from e


def fetch_timeseries_data(request: DataRequest) -> pd.DataFrame:
    """
    時系列データを取得（datetime, value列を持つDataFrame）
    
    Args:
        request: データ取得リクエスト
    
    Returns:
        標準化されたDataFrame（datetime, value, display_dt, sheet_year列）
    """
    log_function_call(logger, "fetch_timeseries_data", 
                     code=request.code, mode=request.mode, 
                     granularity=request.granularity,
                     period=f"{request.start_year}/{request.start_month}-{request.end_year}/{request.end_month}")
    
    try:
        if request.granularity == "hour":
            df = _fetch_hourly_data(request)
        else:
            df = _fetch_daily_data(request)
            
        log_function_result(logger, "fetch_timeseries_data", 
                          f"時系列データ取得完了: {len(df)}件")
        return df
        
    except Exception as e:
        log_error_with_context(logger, e, 
                             f"時系列データ取得エラー (code={request.code})")
        raise


def _fetch_hourly_data(request: DataRequest) -> pd.DataFrame:
    """
    時間次データを取得（既存のmain_datetime.pyロジックを統合）
    """
    import calendar
    from datetime import datetime, timedelta
    
    # 月リストと期間設定（既存ロジックを移植）
    month_list = ["0101","0201","0301","0401","0501","0601",
                  "0701","0801","0901","1001","1101","1201"]
    
    # 月インデックス辞書
    month_dic = {'1月':0, '2月':1, '3月':2, '4月':3, '5月':4, '6月':5, 
                 '7月':6, '8月':7, '9月':8, '10月':9, '11月':10, '12月':11}
    
    # 開始～終了までの月数を計算
    M1 = f"{request.start_month}月"
    M2 = f"{request.end_month}月"
    Y1 = str(request.start_year)
    Y2 = str(request.end_year)
    
    i = month_dic[M1]
    total = (month_dic[M2] - month_dic[M1] + 1) + (int(Y2) - int(Y1)) * 12
    
    new_month = []
    for _ in range(total):
        new_month.append(month_list[i])
        i = (i + 1) % 12

    # 年またぎを考慮してYYYYMMDD文字列を作成
    url_month = []
    kariY = int(Y1)
    for m in new_month:
        url_month.append(f"{kariY}{m}")
        if m == "1201":
            kariY += 1

    # データ取得・Elemリスト構築
    elem_list = []
    for um in url_month:
        values = _fetch_data_for_period(request.code, request.mode, um, f"{Y2}1231", "hour")
        elem_list.extend(values)
        
        # S・Uモードの場合、最後の要素を削除（既存ロジック）
        if request.mode in ["S", "U"] and elem_list:
            elem_list.pop()
    
    if not elem_list:
        raise ValueError("指定期間のデータが取得できませんでした")

    # 日時インデックスの作成
    year_end = int(Y2)
    month_end = int(new_month[-1][:2])
    last_day = calendar.monthrange(year_end, month_end)[1]
    
    year_start = int(Y1)
    month_start = int(new_month[0][:2])
    start_date = datetime(year_start, month_start, 1, 0, 0)

    # １時間ごとのDatetimeIndexを要素数に合わせて生成
    data_date = pd.date_range(start=start_date, periods=len(elem_list), freq='h')

    # DataFrameを作成
    df = pd.DataFrame({'value': elem_list}, index=data_date)
    df = df.reset_index().rename(columns={'index': 'datetime'})
    
    # 1:00 ~ 0:00 方式（表示・集計ともに＋1時間シフト）
    df['display_dt'] = df['datetime'] + pd.to_timedelta(1, 'h')
    
    # 所属年は元の測定時刻ベース
    df['sheet_year'] = df['datetime'].dt.year
    
    # 値列を数値型に変換（pd.to_numeric(errors="coerce")による数値化を標準化）
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    
    return df


def _fetch_daily_data(request: DataRequest) -> pd.DataFrame:
    """
    日次データを取得（既存のdatemode.pyロジックを統合）
    """
    import calendar
    from datetime import datetime
    
    # 年単位でデータ取得・日付インデックス化
    years = list(range(request.start_year, request.end_year + 1))
    all_values, all_dates = [], []
    
    for year in years:
        start_date = f"{year}0101"
        end_date = f"{year}1231"
        values = _fetch_data_for_period(request.code, request.mode, start_date, end_date, "day")
        
        last = calendar.monthrange(year, 12)[1]
        dates = pd.date_range(start=f"{year}-01-01", end=f"{year}-12-{last}", freq="D")
        n = min(len(dates), len(values))
        all_dates += list(dates[:n])
        all_values += values[:n]

    # DataFrame作成・期間フィルタリング
    df = pd.DataFrame({'value': all_values}, index=all_dates)
    
    start_dt = datetime(request.start_year, request.start_month, 1)
    end_dt = datetime(request.end_year, request.end_month, 
                     calendar.monthrange(request.end_year, request.end_month)[1])
    
    df = df[(df.index >= start_dt) & (df.index <= end_dt)]
    df.sort_index(inplace=True)
    
    # DataFrameを標準形式に変換
    df = df.reset_index().rename(columns={'index': 'datetime'})
    
    # 日次データの場合、display_dtはdatetimeと同じ
    df['display_dt'] = df['datetime']
    
    # 所属年
    df['sheet_year'] = df['datetime'].dt.year
    
    # 値列を数値型に変換
    df['value'] = pd.to_numeric(df['value'], errors='coerce')
    
    return df