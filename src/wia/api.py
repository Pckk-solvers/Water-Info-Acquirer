"""
統合実行API

データ取得・Excel出力の統合処理を提供
"""

from pathlib import Path
from typing import List, Tuple, Optional
from .data_source import fetch_station_info, fetch_timeseries_data
from .excel_writer import write_timeseries_excel
from .types import DataRequest, ExcelOptions, ChartConfig
from .errors import EmptyDataError, WaterInfoAcquirerError, NetworkError, ParseError
from .constants import MODE_LABELS, MODE_UNITS
from .logging_config import get_logger, log_function_call, log_function_result, log_error_with_context
from .exception_handler import ExceptionHandler

# ロガー取得
logger = get_logger(__name__)


def execute_data_acquisition(
    codes: List[str],
    start_year: int,
    start_month: int,
    end_year: int,
    end_month: int,
    mode: str,
    granularity: str = "hour",
    single_sheet: bool = False,
    exception_handler: Optional[ExceptionHandler] = None
) -> Tuple[List[Path], List[Tuple[str, str]]]:
    """
    複数観測所のデータ取得・Excel出力の統合処理
    
    Args:
        codes: 観測所コードリスト
        start_year: 開始年
        start_month: 開始月
        end_year: 終了年
        end_month: 終了月
        mode: モード（S/R/U）
        granularity: 粒度（hour/day）
        single_sheet: 全期間シート挿入フラグ
        exception_handler: 例外ハンドラー（Noneの場合はデフォルト処理）
        
    Returns:
        Tuple[List[Path], List[Tuple[str, str]]]: 
            - 生成されたExcelファイルのパスリスト
            - エラーが発生した観測所のリスト（コード, エラーメッセージ）
    """
    results = []
    errors = []
    
    logger.info(f"データ取得開始: {len(codes)}観測所, モード={mode}, 期間={start_year}/{start_month}-{end_year}/{end_month}")
    
    for i, code in enumerate(codes, 1):
        logger.info(f"処理中 ({i}/{len(codes)}): 観測所コード {code}")
        
        try:
            file_path = execute_single_station(
                code=code,
                start_year=start_year,
                start_month=start_month,
                end_year=end_year,
                end_month=end_month,
                mode=mode,
                granularity=granularity,
                single_sheet=single_sheet
            )
            results.append(file_path)
            logger.info(f"観測所 {code}: Excel出力完了 - {file_path}")
            
        except Exception as e:
            # 統一例外ハンドリング
            context = f"観測所 {code} の処理"
            
            if exception_handler:
                # 例外ハンドラーが提供されている場合は使用
                can_continue = exception_handler.handle_exception(e, context)
                
                # エラーリストに追加（ユーザー向けメッセージ）
                from .errors import get_user_friendly_message
                user_message = get_user_friendly_message(e)
                errors.append((code, f"観測所コード {code}：{user_message}"))
                
                if not can_continue:
                    # 継続不可能な場合は処理を中断
                    logger.error("継続不可能なエラーのため処理を中断します")
                    break
            else:
                # デフォルト処理（統一例外ハンドラーを作成して使用）
                from .exception_handler import ExceptionHandler
                default_handler = ExceptionHandler(is_gui_mode=False)
                can_continue = default_handler.handle_exception(e, context)
                
                # エラーリストに追加（ユーザー向けメッセージ）
                from .errors import get_user_friendly_message
                user_message = get_user_friendly_message(e)
                errors.append((code, f"観測所コード {code}：{user_message}"))
                
                if not can_continue:
                    # 継続不可能な場合は処理を中断
                    logger.error("継続不可能なエラーのため処理を中断します")
                    break
    
    logger.info(f"データ取得完了: 成功={len(results)}件, エラー={len(errors)}件")
    
    return results, errors


def execute_single_station(
    code: str,
    start_year: int,
    start_month: int,
    end_year: int,
    end_month: int,
    mode: str,
    granularity: str = "hour",
    single_sheet: bool = False
) -> Path:
    """
    単一観測所のデータ取得・Excel出力
    
    Args:
        code: 観測所コード
        start_year: 開始年
        start_month: 開始月
        end_year: 終了年
        end_month: 終了月
        mode: モード（S/R/U）
        granularity: 粒度（hour/day）
        single_sheet: 全期間シート挿入フラグ
        
    Returns:
        Path: 生成されたExcelファイルのパス
        
    Raises:
        EmptyDataError: データが空の場合
        NetworkError: ネットワークエラー
        ParseError: 解析エラー
        WaterInfoAcquirerError: その他のエラー
    """
    logger.debug(f"単一観測所処理開始: {code}")
    
    try:
        # データ取得リクエスト作成
        request = DataRequest(
            code=code,
            start_year=start_year,
            start_month=start_month,
            end_year=end_year,
            end_month=end_month,
            mode=mode,
            granularity=granularity
        )
        
        # 観測所情報取得
        station_info = fetch_station_info(code, mode)
        logger.debug(f"観測所情報取得完了: {station_info.name}")
        
        # 時系列データ取得
        df = fetch_timeseries_data(request)
        logger.debug(f"時系列データ取得完了: {len(df)}件")
        
        # Excel出力オプション設定
        chart_config = ChartConfig(
            title=f"{station_info.name} ({MODE_LABELS[mode]})",
            y_axis_label=f"{MODE_LABELS[mode]}[{MODE_UNITS[mode]}]",
            x_axis_format="m"
        )
        
        options = ExcelOptions(
            single_sheet=single_sheet,
            include_summary=True,
            chart_config=chart_config
        )
        
        # Excel出力
        file_path = write_timeseries_excel(df, station_info, request, options)
        logger.debug(f"Excel出力完了: {file_path}")
        
        return file_path
        
    except (EmptyDataError, NetworkError, ParseError):
        # 既知のエラーはそのまま再発生
        raise
    except Exception as e:
        # 想定外エラーはWaterInfoAcquirerErrorでラップ
        raise WaterInfoAcquirerError(f"観測所コード {code} の処理でエラーが発生しました: {str(e)}") from e