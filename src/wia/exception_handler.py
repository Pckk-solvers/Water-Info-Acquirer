"""
例外ハンドリングの統一

GUI/CLI別の例外対応、ログ記録、短文通知の統一処理
"""

import sys
from typing import Callable, Optional, Any
from .logging_config import get_logger, log_error_with_context
from .errors import (
    WaterInfoAcquirerError, EmptyDataError, NetworkError, ParseError,
    is_business_exception, is_system_exception, 
    get_user_friendly_message, get_exit_code
)

# ロガー取得
logger = get_logger(__name__)


class ExceptionHandler:
    """例外ハンドリングの統一クラス"""
    
    def __init__(self, is_gui_mode: bool = True):
        """
        初期化
        
        Args:
            is_gui_mode: GUIモードかどうか
        """
        self.is_gui_mode = is_gui_mode
        self.error_display_func: Optional[Callable[[str], None]] = None
    
    def set_error_display_func(self, func: Callable[[str], None]):
        """
        エラー表示関数を設定
        
        Args:
            func: エラー表示関数（メッセージを受け取る）
        """
        self.error_display_func = func
    
    def handle_exception(
        self, 
        error: Exception, 
        context: str = "",
        exit_on_error: bool = False
    ) -> bool:
        """
        例外の統一ハンドリング
        
        Args:
            error: 例外オブジェクト
            context: エラーのコンテキスト情報
            exit_on_error: エラー時にプログラムを終了するかどうか
            
        Returns:
            bool: 処理を継続できる場合True
        """
        # ログ出力（要件4.4対応）
        self._log_exception(error, context)
        
        # ユーザー向けメッセージ取得
        user_message = get_user_friendly_message(error)
        
        # GUI/CLI別の表示処理（要件4.3、4.5対応）
        if self.is_gui_mode:
            self._handle_gui_error(user_message, error)
        else:
            self._handle_cli_error(user_message, error, exit_on_error)
        
        # 継続可能性の判定
        return self._can_continue(error)
    
    def _log_exception(self, error: Exception, context: str):
        """
        例外のログ出力（要件4.4対応）
        想定外例外は詳細ログ記録、業務例外・システム例外は適切なレベルで記録
        """
        if is_business_exception(error):
            # 業務例外は警告レベル（ユーザー操作に起因する想定内エラー）
            logger.warning(f"業務例外 - {context}: {str(error)}")
        elif is_system_exception(error):
            # システム例外はエラーレベル（ネットワーク・解析エラー等）
            log_error_with_context(logger, error, context, include_traceback=False)
        else:
            # 想定外例外は詳細ログ（要件4.4: ログに記録し、短文で通知）
            logger.error(f"想定外例外 - {context}: {type(error).__name__}: {str(error)}")
            log_error_with_context(logger, error, context, include_traceback=True)
    
    def _handle_gui_error(self, message: str, error: Exception):
        """
        GUI用エラーハンドリング（要件4.3、4.5対応）
        業務例外・システム例外・想定外例外すべてポップアップで表示
        """
        if is_business_exception(error):
            # 業務例外：ポップアップ表示（要件4.3）
            logger.info(f"GUI業務例外表示: {message}")
        elif is_system_exception(error):
            # システム例外：ポップアップ表示
            logger.info(f"GUIシステム例外表示: {message}")
        else:
            # 想定外例外：短文通知（要件4.4）
            logger.info(f"GUI想定外例外表示: {message}")
        
        if self.error_display_func:
            self.error_display_func(message)
        else:
            # フォールバック：標準出力
            print(f"エラー: {message}")
    
    def _handle_cli_error(self, message: str, error: Exception, exit_on_error: bool):
        """
        CLI用エラーハンドリング（要件4.3、4.5対応）
        業務例外・システム例外・想定外例外すべて非ゼロ終了コードとメッセージ表示
        """
        # エラーメッセージを標準エラー出力に表示（要件4.3、4.5）
        print(f"エラー: {message}", file=sys.stderr)
        
        # 例外タイプに応じた終了コード設定
        exit_code = get_exit_code(error)
        
        if is_business_exception(error):
            # 業務例外：非ゼロ終了コード（要件4.3）
            logger.info(f"CLI業務例外 - 終了コード: {exit_code}")
        elif is_system_exception(error):
            # システム例外：非ゼロ終了コード
            logger.info(f"CLIシステム例外 - 終了コード: {exit_code}")
        else:
            # 想定外例外：短文通知と非ゼロ終了コード（要件4.4）
            logger.info(f"CLI想定外例外 - 終了コード: {exit_code}")
        
        if exit_on_error:
            logger.info(f"プログラム終了 (終了コード: {exit_code})")
            sys.exit(exit_code)
    
    def _can_continue(self, error: Exception) -> bool:
        """処理継続可能性の判定"""
        # 業務例外は継続可能
        if is_business_exception(error):
            return True
        
        # システム例外は基本的に継続不可
        if is_system_exception(error):
            return False
        
        # その他の例外は継続不可
        return False


def handle_exception_with_context(
    func: Callable,
    context: str,
    handler: ExceptionHandler,
    *args,
    **kwargs
) -> Any:
    """
    関数実行時の例外ハンドリング
    
    Args:
        func: 実行する関数
        context: コンテキスト情報
        handler: 例外ハンドラー
        *args: 関数の引数
        **kwargs: 関数のキーワード引数
        
    Returns:
        Any: 関数の戻り値（例外時はNone）
    """
    try:
        return func(*args, **kwargs)
    except Exception as e:
        handler.handle_exception(e, context)
        return None


def create_gui_exception_handler(error_display_func: Callable[[str], None]) -> ExceptionHandler:
    """
    GUI用例外ハンドラーの作成
    
    Args:
        error_display_func: エラー表示関数
        
    Returns:
        ExceptionHandler: GUI用例外ハンドラー
    """
    handler = ExceptionHandler(is_gui_mode=True)
    handler.set_error_display_func(error_display_func)
    return handler


def create_cli_exception_handler() -> ExceptionHandler:
    """
    CLI用例外ハンドラーの作成
    
    Returns:
        ExceptionHandler: CLI用例外ハンドラー
    """
    return ExceptionHandler(is_gui_mode=False)


# デコレータ関数
def with_exception_handling(handler: ExceptionHandler, context: str = ""):
    """
    例外ハンドリング付きデコレータ
    
    Args:
        handler: 例外ハンドラー
        context: コンテキスト情報
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            return handle_exception_with_context(
                func, context or func.__name__, handler, *args, **kwargs
            )
        return wrapper
    return decorator