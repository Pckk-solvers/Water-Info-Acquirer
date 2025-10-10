"""
Excel出力の共通レイヤ

全期間・年別・summaryシートの統一出力、散布図挿入の共通化
"""

import pandas as pd
from pathlib import Path
from typing import List
from datetime import datetime
import calendar
from .types import StationInfo, DataRequest, ExcelOptions, ChartConfig
from .errors import WaterInfoAcquirerError, EmptyDataError
from .constants import MODE_LABELS, MODE_UNITS, MODE_FILE_SUFFIXES, DEFAULT_CHART_SIZE
from .logging_config import get_logger, log_function_call, log_function_result, log_error_with_context

# ロガー取得
logger = get_logger(__name__)


def generate_filename(
    station_info: StationInfo,
    request: DataRequest
) -> str:
    """
    ファイル名生成の標準化
    
    Args:
        station_info: 観測所情報
        request: データ取得リクエスト
        
    Returns:
        str: 生成されたファイル名
    """
    suffix = MODE_FILE_SUFFIXES[request.mode]
    if request.granularity == "day":
        # 日次データの場合は異なる接尾辞
        suffix_map = {"S": "WD", "R": "QD", "U": "RD"}
        suffix = suffix_map[request.mode]
    
    filename = (
        f"{request.code}_{station_info.name}_"
        f"{request.start_year}年{request.start_month}月-"
        f"{request.end_year}年{request.end_month}月_{suffix}.xlsx"
    )
    return filename


def write_timeseries_excel(
    df: pd.DataFrame,
    station_info: StationInfo,
    request: DataRequest,
    options: ExcelOptions
) -> Path:
    """
    統合Excel出力API
    
    Args:
        df: 時系列データ（datetime, value, display_dt, sheet_year列を持つ）
        station_info: 観測所情報
        request: データ取得リクエスト
        options: Excel出力オプション
        
    Returns:
        Path: 出力されたExcelファイルのパス
        
    Raises:
        EmptyDataError: データが空の場合
        WaterInfoAcquirerError: Excel出力エラー
    """
    log_function_call(logger, "write_timeseries_excel", 
                     code=request.code, station_name=station_info.name,
                     data_count=len(df), single_sheet=options.single_sheet)
    
    # データの空チェック
    if df.empty or df['value'].dropna().empty:
        error_msg = f"観測所コード {request.code}：指定期間に有効なデータが見つかりませんでした"
        logger.warning(error_msg)
        raise EmptyDataError(error_msg)
    
    # ファイル名生成
    filename = generate_filename(station_info, request)
    logger.info(f"Excel出力開始: {filename}")
    
    # チャート設定の準備
    chart_config = options.chart_config
    if chart_config is None:
        mode_label = MODE_LABELS[request.mode]
        unit = MODE_UNITS[request.mode]
        chart_config = ChartConfig(
            title=f"{request.start_year}/{request.start_month}月 - {request.end_year}/{request.end_month}月",
            y_axis_label=f"{mode_label}[{unit}]",
            x_axis_format="m" if request.granularity == "hour" else "yyyy/mm"
        )
    
    try:
        # XlsxWriter で書き出し
        with pd.ExcelWriter(filename, engine='xlsxwriter',
                            datetime_format='yyyy/m/d h:mm' if request.granularity == "hour" else 'yyyy/mm/dd') as writer:
            
            # 全期間シートの作成（single_sheetオプション）
            if options.single_sheet:
                logger.debug("全期間シート作成中")
                create_full_period_sheet(df, writer, chart_config, request)
            
            # 年別シートの作成
            logger.debug("年別シート作成中")
            create_yearly_sheets(df, writer, chart_config, request)
            
            # サマリーシートの作成
            if options.include_summary:
                logger.debug("サマリーシート作成中")
                create_summary_sheet(df, writer, request)
        
        logger.info(f"Excelファイルの作成が完了しました: {filename}")
        log_function_result(logger, "write_timeseries_excel", f"Excel出力完了: {filename}")
        return Path(filename)
        
    except Exception as e:
        log_error_with_context(logger, e, f"Excel出力エラー (file={filename})")
        raise WaterInfoAcquirerError(f"Excel出力エラー: {str(e)}") from e


def create_full_period_sheet(
    df: pd.DataFrame, 
    writer: pd.ExcelWriter, 
    chart_config: ChartConfig,
    request: DataRequest
):
    """
    全期間シート作成
    
    Args:
        df: 時系列データ
        writer: ExcelWriter
        chart_config: チャート設定
        request: データ取得リクエスト
    """
    sheet_name = "全期間"
    
    # 表示用データフレーム作成（display_dt + value列）
    display_df = df[['display_dt', 'value']].copy()
    display_df.columns = ['datetime', MODE_LABELS[request.mode]]
    
    # シートに出力
    display_df.to_excel(writer, sheet_name=sheet_name, index=False)
    
    # ワークシート取得と列幅調整
    ws = writer.sheets[sheet_name]
    ws.set_column('A:A', 20)  # datetime列
    ws.set_column('B:B', 12)  # 値列
    
    # チャート挿入
    insert_chart(ws, display_df, chart_config, 'D2', sheet_name, request)


def create_yearly_sheets(
    df: pd.DataFrame, 
    writer: pd.ExcelWriter, 
    chart_config: ChartConfig,
    request: DataRequest
):
    """
    年別シート作成
    
    Args:
        df: 時系列データ
        writer: ExcelWriter
        chart_config: チャート設定
        request: データ取得リクエスト
    """
    # 年ごとにグループ化してシート作成
    for year, group in df.groupby('sheet_year', sort=True):
        sheet_name = f"{year}年"
        
        # 表示用データフレーム作成
        display_df = group[['display_dt', 'value']].copy()
        display_df.columns = ['datetime', MODE_LABELS[request.mode]]
        
        # シートに出力
        display_df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        # ワークシート取得と列幅調整
        ws = writer.sheets[sheet_name]
        ws.set_column('A:A', 20)  # datetime列
        ws.set_column('B:B', 12)  # 値列
        
        # チャート挿入
        year_chart_config = ChartConfig(
            title=f"{year}年",
            y_axis_label=chart_config.y_axis_label,
            x_axis_format=chart_config.x_axis_format
        )
        insert_chart(ws, display_df, year_chart_config, 'D2', sheet_name, request)


def create_summary_sheet(
    df: pd.DataFrame, 
    writer: pd.ExcelWriter, 
    request: DataRequest
):
    """
    サマリーシート作成
    
    Args:
        df: 時系列データ
        writer: ExcelWriter
        request: データ取得リクエスト
    """
    sheet_name = "summary"
    
    # 日別サマリ用データフレーム作成
    if request.granularity == "hour":
        # 時間次データの場合は日別集計
        daily_summary = create_daily_summary(df, request)
    else:
        # 日次データの場合は月別集計
        daily_summary = create_monthly_summary(df, request)
    
    # 年別サマリ用データフレーム作成
    yearly_summary = create_yearly_summary(df, request)
    
    # 日別サマリをA/B列に出力
    daily_summary.to_excel(
        writer, 
        sheet_name=sheet_name, 
        index=False, 
        startrow=0, 
        startcol=0
    )
    
    # 年別サマリをD〜G列に出力
    yearly_summary.to_excel(
        writer, 
        sheet_name=sheet_name, 
        index=False, 
        startrow=0, 
        startcol=3
    )
    
    # ワークシート取得と列幅調整
    ws = writer.sheets[sheet_name]
    ws.set_column('A:A', 15)  # date列
    ws.set_column('B:B', 12)  # count列
    ws.set_column('D:D', 8)   # year列
    ws.set_column('E:E', 20)  # datetime列
    ws.set_column('F:F', 10)  # 値列
    ws.set_column('G:G', 18)  # count列


def create_daily_summary(df: pd.DataFrame, request: DataRequest) -> pd.DataFrame:
    """日別サマリ作成（時間次データ用）"""
    mode_label = MODE_LABELS[request.mode]
    
    # 日別データフレーム作成
    tmp = pd.DataFrame({
        'date': df['display_dt'].dt.strftime('%Y/%m/%d'),
        mode_label: df['value']
    })
    tmp[mode_label] = pd.to_numeric(tmp[mode_label], errors='coerce')
    
    # 日別の空データ数を集計
    daily_df = (
        tmp.groupby('date')
        .agg(empty_count=(mode_label, lambda s: s.isna().sum()))
        .reset_index()
    )
    
    return daily_df


def create_monthly_summary(df: pd.DataFrame, request: DataRequest) -> pd.DataFrame:
    """月別サマリ作成（日次データ用）"""
    mode_label = MODE_LABELS[request.mode]
    
    # 月別データフレーム作成
    tmp = pd.DataFrame({
        'month': df['display_dt'].dt.strftime('%Y/%m'),
        mode_label: df['value']
    })
    tmp[mode_label] = pd.to_numeric(tmp[mode_label], errors='coerce')
    
    # 月別の空データ数を集計
    monthly_df = (
        tmp.groupby('month')
        .agg(empty_count=(mode_label, lambda s: s.isna().sum()))
        .reset_index()
    )
    
    return monthly_df


def create_yearly_summary(df: pd.DataFrame, request: DataRequest) -> pd.DataFrame:
    """年別サマリ作成"""
    mode_label = MODE_LABELS[request.mode]
    
    year_list = []
    for year, group in df.groupby('sheet_year', sort=True):
        # 非null値がない年はスキップ
        non_null = group['value'].dropna()
        if non_null.empty:
            continue
            
        max_idx = non_null.idxmax()
        ts_max = group.loc[max_idx, 'display_dt'].to_pydatetime()
        val_max = group.loc[max_idx, 'value']
        empty_year = group['value'].isna().sum()
        
        year_list.append([year, ts_max, val_max, empty_year])
    
    year_summary_df = pd.DataFrame(
        year_list,
        columns=['year', 'year_max_datetime', mode_label, 'year_empty_count']
    )
    
    return year_summary_df


def insert_chart(
    worksheet, 
    df: pd.DataFrame, 
    config: ChartConfig, 
    position: str,
    sheet_name: str,
    request: DataRequest
):
    """
    散布図挿入の共通ロジック
    
    Args:
        worksheet: ワークシート
        df: データ
        config: チャート設定
        position: 挿入位置
        sheet_name: シート名
        request: データ取得リクエスト
    """
    # チャート作成
    chart = worksheet.book.add_chart({
        'type': 'scatter',
        'subtype': 'straight_with_markers'
    })
    
    max_row = len(df) + 1
    
    # データ系列の追加
    chart.add_series({
        'name': sheet_name,
        'categories': [sheet_name, 1, 0, max_row-1, 0],  # A列（datetime）
        'values': [sheet_name, 1, 1, max_row-1, 1],      # B列（値）
        'marker': {'type': 'none'},
        'line': {'width': 1.5},
    })
    
    # X軸設定
    if request.granularity == "hour":
        # 時間次データの場合
        min_dt = df['datetime'].min()
        max_dt = df['datetime'].max()
        
        # 月初を基準にした範囲設定
        from datetime import datetime
        
        def month_floor(dt: datetime) -> datetime:
            return datetime(dt.year, dt.month, 1)
        
        def shift_month(dt: datetime, n: int) -> datetime:
            y = dt.year + (dt.month - 1 + n) // 12
            m = (dt.month - 1 + n) % 12 + 1
            return datetime(y, m, 1)
        
        xmin = shift_month(month_floor(min_dt), -1)
        xmax = shift_month(month_floor(max_dt), +2)
        
        chart.set_x_axis({
            'name': '日時[月]',
            'date_axis': True,
            'num_format': 'm',
            'major_unit': 31,
            'min': xmin,
            'max': xmax,
            'major_unit_type': 'months',
            'major_gridlines': {'visible': True},
            'label_position': 'low'
        })
    else:
        # 日次データの場合
        chart.set_x_axis({
            'name': '日時[年/月]',
            'date_axis': True,
            'num_format': 'yyyy/mm',
            'major_unit': 185,
            'major_gridlines': {'visible': True}
        })
    
    # Y軸設定
    chart.set_y_axis({'name': config.y_axis_label})
    
    # タイトル設定
    chart.set_title({'name': config.title})
    
    # 凡例設定
    chart.set_legend({'position': 'none'})
    
    # サイズ設定
    chart.set_size({'width': config.size[0], 'height': config.size[1]})
    
    # チャート挿入
    worksheet.insert_chart(position, chart)