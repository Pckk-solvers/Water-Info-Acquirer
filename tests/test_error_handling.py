"""
エラーハンドリングのテスト実装

各例外クラスの動作テスト、GUI・CLI別のエラー対応テスト、
ログ出力の確認テスト
"""

import pytest
import sys
import logging
from unittest.mock import Mock, patch, MagicMock
from io import StringIO
from pathlib import Path

from src.wia.errors import (
    WaterInfoAcquirerError, EmptyDataError, NetworkError, ParseError,
    EmptyExcelWarning, is_business_exception, is_system_exception,
    get_user_friendly_message, get_exit_code
)
from src.wia.exception_handler import (
    ExceptionHandler, handle_exception_with_context,
    create_gui_exception_handler, create_cli_exception_handler,
    with_exception_handling
)
from src.wia.logging_config import (
    setup_logging, get_logger, log_function_call,
    log_function_result, log_error_with_context
)


class TestExceptionClasses:
    """例外クラスの動作テスト"""
    
    def test_water_info_acquirer_error_base(self):
        """基底例外クラスのテスト"""
        error = WaterInfoAcquirerError("テストエラー")
        assert str(error) == "テストエラー"
        assert isinstance(error, Exception)
    
    def test_empty_data_error_default_message(self):
        """EmptyDataErrorのデフォルトメッセージテスト"""
        error = EmptyDataError()
        assert str(error) == "取得したデータが空です"
        assert error.message == "取得したデータが空です"
        assert isinstance(error, WaterInfoAcquirerError)
    
    def test_empty_data_error_custom_message(self):
        """EmptyDataErrorのカスタムメッセージテスト"""
        custom_message = "指定期間に有効なデータが見つかりませんでした"
        error = EmptyDataError(custom_message)
        assert str(error) == custom_message
        assert error.message == custom_message
    
    def test_network_error_default_message(self):
        """NetworkErrorのデフォルトメッセージテスト"""
        error = NetworkError()
        assert str(error) == "ネットワーク接続エラーが発生しました"
        assert error.message == "ネットワーク接続エラーが発生しました"
        assert isinstance(error, WaterInfoAcquirerError)
    
    def test_network_error_custom_message(self):
        """NetworkErrorのカスタムメッセージテスト"""
        custom_message = "タイムアウトが発生しました"
        error = NetworkError(custom_message)
        assert str(error) == custom_message
        assert error.message == custom_message
    
    def test_parse_error_default_message(self):
        """ParseErrorのデフォルトメッセージテスト"""
        error = ParseError()
        assert str(error) == "データの解析に失敗しました"
        assert error.message == "データの解析に失敗しました"
        assert isinstance(error, WaterInfoAcquirerError)
    
    def test_parse_error_custom_message(self):
        """ParseErrorのカスタムメッセージテスト"""
        custom_message = "HTMLの構造が想定と異なります"
        error = ParseError(custom_message)
        assert str(error) == custom_message
        assert error.message == custom_message
    
    def test_empty_excel_warning_compatibility(self):
        """EmptyExcelWarningの互換性テスト"""
        error = EmptyExcelWarning("互換性テスト")
        assert isinstance(error, EmptyDataError)
        assert isinstance(error, WaterInfoAcquirerError)
        assert str(error) == "互換性テスト"


class TestExceptionClassification:
    """例外分類関数のテスト"""
    
    def test_is_business_exception_true_cases(self):
        """業務例外判定のTrueケース"""
        assert is_business_exception(EmptyDataError()) is True
        assert is_business_exception(EmptyExcelWarning()) is True
    
    def test_is_business_exception_false_cases(self):
        """業務例外判定のFalseケース"""
        assert is_business_exception(NetworkError()) is False
        assert is_business_exception(ParseError()) is False
        assert is_business_exception(ValueError()) is False
        assert is_business_exception(Exception()) is False
    
    def test_is_system_exception_true_cases(self):
        """システム例外判定のTrueケース"""
        assert is_system_exception(NetworkError()) is True
        assert is_system_exception(ParseError()) is True
    
    def test_is_system_exception_false_cases(self):
        """システム例外判定のFalseケース"""
        assert is_system_exception(EmptyDataError()) is False
        assert is_system_exception(ValueError()) is False
        assert is_system_exception(Exception()) is False
    
    def test_get_user_friendly_message_business_exception(self):
        """業務例外のユーザー向けメッセージテスト"""
        error = EmptyDataError("データが見つかりません")
        message = get_user_friendly_message(error)
        assert message == "データが見つかりません"
    
    def test_get_user_friendly_message_system_exception(self):
        """システム例外のユーザー向けメッセージテスト"""
        error = NetworkError("接続に失敗しました")
        message = get_user_friendly_message(error)
        assert message == "接続に失敗しました"
    
    def test_get_user_friendly_message_app_exception(self):
        """アプリケーション例外のユーザー向けメッセージテスト"""
        error = WaterInfoAcquirerError("アプリケーションエラー")
        message = get_user_friendly_message(error)
        assert message == "アプリケーションエラー"
    
    def test_get_user_friendly_message_unexpected_exception(self):
        """想定外例外のユーザー向けメッセージテスト"""
        error = ValueError("想定外のエラー")
        message = get_user_friendly_message(error)
        assert message == "想定外のエラーが発生しました。ログファイルを確認してください。"
    
    def test_get_exit_code_business_exception(self):
        """業務例外の終了コードテスト"""
        assert get_exit_code(EmptyDataError()) == 2
        assert get_exit_code(EmptyExcelWarning()) == 2
    
    def test_get_exit_code_system_exception(self):
        """システム例外の終了コードテスト"""
        assert get_exit_code(NetworkError()) == 3
        assert get_exit_code(ParseError()) == 3
    
    def test_get_exit_code_app_exception(self):
        """アプリケーション例外の終了コードテスト"""
        assert get_exit_code(WaterInfoAcquirerError()) == 4
    
    def test_get_exit_code_unexpected_exception(self):
        """想定外例外の終了コードテスト"""
        assert get_exit_code(ValueError()) == 1
        assert get_exit_code(Exception()) == 1


class TestExceptionHandler:
    """ExceptionHandlerクラスのテスト"""
    
    def test_init_gui_mode(self):
        """GUIモード初期化テスト"""
        handler = ExceptionHandler(is_gui_mode=True)
        assert handler.is_gui_mode is True
        assert handler.error_display_func is None
    
    def test_init_cli_mode(self):
        """CLIモード初期化テスト"""
        handler = ExceptionHandler(is_gui_mode=False)
        assert handler.is_gui_mode is False
        assert handler.error_display_func is None
    
    def test_set_error_display_func(self):
        """エラー表示関数設定テスト"""
        handler = ExceptionHandler()
        mock_func = Mock()
        
        handler.set_error_display_func(mock_func)
        assert handler.error_display_func == mock_func
    
    @patch('src.wia.exception_handler.logger')
    def test_handle_gui_business_exception(self, mock_logger):
        """GUI業務例外ハンドリングテスト"""
        handler = ExceptionHandler(is_gui_mode=True)
        mock_display = Mock()
        handler.set_error_display_func(mock_display)
        
        error = EmptyDataError("テストデータが空です")
        result = handler.handle_exception(error, "テストコンテキスト")
        
        # 継続可能であることを確認
        assert result is True
        
        # エラー表示関数が呼ばれることを確認
        mock_display.assert_called_once_with("テストデータが空です")
        
        # ログが適切に出力されることを確認
        mock_logger.warning.assert_called_once()
        mock_logger.info.assert_called_once()
    
    @patch('src.wia.exception_handler.logger')
    @patch('sys.stderr', new_callable=StringIO)
    def test_handle_cli_business_exception(self, mock_stderr, mock_logger):
        """CLI業務例外ハンドリングテスト"""
        handler = ExceptionHandler(is_gui_mode=False)
        
        error = EmptyDataError("テストデータが空です")
        result = handler.handle_exception(error, "テストコンテキスト")
        
        # 継続可能であることを確認
        assert result is True
        
        # 標準エラー出力にメッセージが出力されることを確認
        assert "エラー: テストデータが空です" in mock_stderr.getvalue()
        
        # ログが適切に出力されることを確認
        mock_logger.warning.assert_called_once()
        mock_logger.info.assert_called_once()
    
    @patch('src.wia.exception_handler.logger')
    def test_handle_gui_system_exception(self, mock_logger):
        """GUIシステム例外ハンドリングテスト"""
        handler = ExceptionHandler(is_gui_mode=True)
        mock_display = Mock()
        handler.set_error_display_func(mock_display)
        
        error = NetworkError("ネットワークエラー")
        result = handler.handle_exception(error, "テストコンテキスト")
        
        # 継続不可であることを確認
        assert result is False
        
        # エラー表示関数が呼ばれることを確認
        mock_display.assert_called_once_with("ネットワークエラー")
    
    @patch('src.wia.exception_handler.logger')
    @patch('sys.stderr', new_callable=StringIO)
    @patch('sys.exit')
    def test_handle_cli_system_exception_with_exit(self, mock_exit, mock_stderr, mock_logger):
        """CLIシステム例外ハンドリング（終了あり）テスト"""
        handler = ExceptionHandler(is_gui_mode=False)
        
        error = NetworkError("ネットワークエラー")
        handler.handle_exception(error, "テストコンテキスト", exit_on_error=True)
        
        # 適切な終了コードでsys.exitが呼ばれることを確認
        mock_exit.assert_called_once_with(3)  # システム例外の終了コード
        
        # 標準エラー出力にメッセージが出力されることを確認
        assert "エラー: ネットワークエラー" in mock_stderr.getvalue()
    
    @patch('src.wia.exception_handler.logger')
    def test_handle_gui_unexpected_exception(self, mock_logger):
        """GUI想定外例外ハンドリングテスト"""
        handler = ExceptionHandler(is_gui_mode=True)
        mock_display = Mock()
        handler.set_error_display_func(mock_display)
        
        error = ValueError("想定外のエラー")
        result = handler.handle_exception(error, "テストコンテキスト")
        
        # 継続不可であることを確認
        assert result is False
        
        # 短文通知が表示されることを確認
        expected_message = "想定外のエラーが発生しました。ログファイルを確認してください。"
        mock_display.assert_called_once_with(expected_message)
        
        # 詳細ログが出力されることを確認
        mock_logger.error.assert_called()
    
    @patch('src.wia.exception_handler.logger')
    @patch('sys.stderr', new_callable=StringIO)
    @patch('sys.exit')
    def test_handle_cli_unexpected_exception_with_exit(self, mock_exit, mock_stderr, mock_logger):
        """CLI想定外例外ハンドリング（終了あり）テスト"""
        handler = ExceptionHandler(is_gui_mode=False)
        
        error = ValueError("想定外のエラー")
        handler.handle_exception(error, "テストコンテキスト", exit_on_error=True)
        
        # 適切な終了コードでsys.exitが呼ばれることを確認
        mock_exit.assert_called_once_with(1)  # 想定外例外の終了コード
        
        # 短文通知が標準エラー出力されることを確認
        expected_message = "想定外のエラーが発生しました。ログファイルを確認してください。"
        assert f"エラー: {expected_message}" in mock_stderr.getvalue()


class TestExceptionHandlerHelpers:
    """例外ハンドラーのヘルパー関数テスト"""
    
    def test_handle_exception_with_context_success(self):
        """正常実行時のコンテキスト付き例外ハンドリングテスト"""
        def test_func(x, y):
            return x + y
        
        handler = Mock()
        result = handle_exception_with_context(test_func, "テスト関数", handler, 1, 2)
        
        assert result == 3
        handler.handle_exception.assert_not_called()
    
    def test_handle_exception_with_context_exception(self):
        """例外発生時のコンテキスト付き例外ハンドリングテスト"""
        def test_func():
            raise ValueError("テストエラー")
        
        handler = Mock()
        result = handle_exception_with_context(test_func, "テスト関数", handler)
        
        assert result is None
        handler.handle_exception.assert_called_once()
        args = handler.handle_exception.call_args[0]
        assert isinstance(args[0], ValueError)
        assert args[1] == "テスト関数"
    
    def test_create_gui_exception_handler(self):
        """GUI例外ハンドラー作成テスト"""
        mock_display = Mock()
        handler = create_gui_exception_handler(mock_display)
        
        assert handler.is_gui_mode is True
        assert handler.error_display_func == mock_display
    
    def test_create_cli_exception_handler(self):
        """CLI例外ハンドラー作成テスト"""
        handler = create_cli_exception_handler()
        
        assert handler.is_gui_mode is False
        assert handler.error_display_func is None
    
    def test_with_exception_handling_decorator_success(self):
        """例外ハンドリングデコレータ（正常実行）テスト"""
        handler = Mock()
        
        @with_exception_handling(handler, "デコレータテスト")
        def test_func(x):
            return x * 2
        
        result = test_func(5)
        assert result == 10
        handler.handle_exception.assert_not_called()
    
    def test_with_exception_handling_decorator_exception(self):
        """例外ハンドリングデコレータ（例外発生）テスト"""
        handler = Mock()
        
        @with_exception_handling(handler, "デコレータテスト")
        def test_func():
            raise ValueError("デコレータエラー")
        
        result = test_func()
        assert result is None
        handler.handle_exception.assert_called_once()


class TestLoggingConfig:
    """ログ設定のテスト"""
    
    def setup_method(self):
        """各テストメソッドの前に実行される初期化"""
        # 既存のハンドラーをクリア
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
    
    def teardown_method(self):
        """各テストメソッドの後に実行されるクリーンアップ"""
        # テスト後のクリーンアップ
        root_logger = logging.getLogger()
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
    
    def test_setup_logging_default(self):
        """デフォルト設定でのログ設定テスト"""
        logger = setup_logging()
        
        assert logger.level == logging.INFO
        assert len(logger.handlers) == 2  # コンソール + ファイル
        
        # ハンドラーの種類確認
        handler_types = [type(h).__name__ for h in logger.handlers]
        assert 'StreamHandler' in handler_types
        assert 'RotatingFileHandler' in handler_types
    
    def test_setup_logging_no_file(self):
        """ファイル出力なしのログ設定テスト"""
        logger = setup_logging(log_file=None)
        
        assert logger.level == logging.INFO
        assert len(logger.handlers) == 1  # コンソールのみ
        
        # ハンドラーの種類確認
        handler_types = [type(h).__name__ for h in logger.handlers]
        assert 'StreamHandler' in handler_types
        assert 'RotatingFileHandler' not in handler_types
    
    def test_setup_logging_custom_level(self):
        """カスタムレベルでのログ設定テスト"""
        logger = setup_logging(log_level="DEBUG", log_file=None)
        
        assert logger.level == logging.DEBUG
        
        # ハンドラーのレベルも確認
        for handler in logger.handlers:
            assert handler.level == logging.DEBUG
    
    def test_get_logger(self):
        """モジュール用ロガー取得テスト"""
        logger = get_logger("test_module")
        
        assert logger.name == "test_module"
        assert isinstance(logger, logging.Logger)
    
    @patch('src.wia.logging_config.logging.getLogger')
    def test_log_function_call(self, mock_get_logger):
        """関数呼び出しログテスト"""
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        
        logger = get_logger("test")
        log_function_call(logger, "test_function", arg1="value1", arg2=123)
        
        # デバッグレベルでログが出力されることを確認
        logger.debug.assert_called_once()
        call_args = logger.debug.call_args[0][0]
        assert "関数呼び出し: test_function" in call_args
        assert "arg1=value1" in call_args
        assert "arg2=123" in call_args
    
    @patch('src.wia.logging_config.logging.getLogger')
    def test_log_function_result(self, mock_get_logger):
        """関数結果ログテスト"""
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        
        logger = get_logger("test")
        log_function_result(logger, "test_function", "成功: 5件処理")
        
        # デバッグレベルでログが出力されることを確認
        logger.debug.assert_called_once()
        call_args = logger.debug.call_args[0][0]
        assert "関数完了: test_function - 成功: 5件処理" in call_args
    
    @patch('src.wia.logging_config.logging.getLogger')
    def test_log_error_with_context_with_traceback(self, mock_get_logger):
        """トレースバック付きエラーログテスト"""
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        
        logger = get_logger("test")
        error = ValueError("テストエラー")
        log_error_with_context(logger, error, "テストコンテキスト", include_traceback=True)
        
        # エラーレベルでログが出力されることを確認
        logger.error.assert_called_once()
        call_args = logger.error.call_args
        assert "テストコンテキスト: ValueError: テストエラー" in call_args[0][0]
        assert call_args[1]['exc_info'] is True
    
    @patch('src.wia.logging_config.logging.getLogger')
    def test_log_error_with_context_without_traceback(self, mock_get_logger):
        """トレースバックなしエラーログテスト"""
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        
        logger = get_logger("test")
        error = NetworkError("ネットワークエラー")
        log_error_with_context(logger, error, "テストコンテキスト", include_traceback=False)
        
        # エラーレベルでログが出力されることを確認
        logger.error.assert_called_once()
        call_args = logger.error.call_args
        assert "テストコンテキスト: NetworkError: ネットワークエラー" in call_args[0][0]
        # exc_infoが設定されていないことを確認
        assert len(call_args[1]) == 0 or call_args[1].get('exc_info') is None


if __name__ == "__main__":
    pytest.main([__file__])