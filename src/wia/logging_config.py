"""
ログシステムの設定

標準loggingモジュールの設定（INFO/ERROR、ファイル+コンソール）
"""

import logging
import logging.handlers
from pathlib import Path
from typing import Optional


def setup_logging(
    log_level: str = "INFO",
    log_file: Optional[str] = "water_info_acquirer.log",
    max_bytes: int = 10 * 1024 * 1024,  # 10MB
    backup_count: int = 5
) -> logging.Logger:
    """
    ログシステムの設定
    
    Args:
        log_level: ログレベル（DEBUG/INFO/WARNING/ERROR/CRITICAL）
        log_file: ログファイル名（Noneの場合はファイル出力なし）
        max_bytes: ログファイルの最大サイズ（バイト）
        backup_count: ローテーション時の保持ファイル数
        
    Returns:
        logging.Logger: 設定されたルートロガー
    """
    # ルートロガーの取得
    root_logger = logging.getLogger()
    
    # 既存のハンドラーをクリア（重複設定を防ぐ）
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # ログレベル設定
    numeric_level = getattr(logging, log_level.upper(), logging.INFO)
    root_logger.setLevel(numeric_level)
    
    # ログフォーマット設定
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    # コンソールハンドラーの設定
    console_handler = logging.StreamHandler()
    console_handler.setLevel(numeric_level)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)
    
    # ファイルハンドラーの設定（ローテーション対応）
    if log_file:
        file_handler = logging.handlers.RotatingFileHandler(
            filename=log_file,
            maxBytes=max_bytes,
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(numeric_level)
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)
    
    return root_logger


def get_logger(name: str) -> logging.Logger:
    """
    モジュール用ロガーの取得
    
    Args:
        name: ロガー名（通常は__name__を使用）
        
    Returns:
        logging.Logger: モジュール用ロガー
    """
    return logging.getLogger(name)


def log_function_call(logger: logging.Logger, func_name: str, **kwargs):
    """
    関数呼び出しのログ出力
    
    Args:
        logger: ロガー
        func_name: 関数名
        **kwargs: 引数情報
    """
    args_str = ", ".join([f"{k}={v}" for k, v in kwargs.items()])
    logger.debug(f"関数呼び出し: {func_name}({args_str})")


def log_function_result(logger: logging.Logger, func_name: str, result_info: str):
    """
    関数結果のログ出力
    
    Args:
        logger: ロガー
        func_name: 関数名
        result_info: 結果情報
    """
    logger.debug(f"関数完了: {func_name} - {result_info}")


def log_error_with_context(
    logger: logging.Logger, 
    error: Exception, 
    context: str,
    include_traceback: bool = True
):
    """
    エラーのコンテキスト付きログ出力
    
    Args:
        logger: ロガー
        error: 例外オブジェクト
        context: エラーのコンテキスト情報
        include_traceback: スタックトレースを含めるかどうか
    """
    error_msg = f"{context}: {type(error).__name__}: {str(error)}"
    
    if include_traceback:
        logger.error(error_msg, exc_info=True)
    else:
        logger.error(error_msg)