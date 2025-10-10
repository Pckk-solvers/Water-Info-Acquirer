"""
例外ハンドリングの統一テスト

要件4.3、4.4、4.5の実装を検証
"""

import pytest
import sys
from io import StringIO
from unittest.mock import Mock, patch, MagicMock

from src.wia.exception_handler import (
    ExceptionHandler, 
    create_gui_exception_handler, 
    create_cli_exception_handler,
    handle_exception_with_context
)
from src.wia.errors import (
    EmptyDataError, NetworkError, ParseError, WaterInfoAcquirerError
)


class TestExceptionHandler:
    """例外ハンドラーのテスト"""
    
    def test_gui_business_exception_handling(self):
        """GUI業務例外ハンドリングのテスト（要件4.3）"""
        # エラー表示関数のモック
        error_display_mock = Mock()
        
        # GUI例外ハンドラー作成
        handler = create_gui_exception_handler(error_display_mock)
        
        # 業務例外を処理
        error = EmptyDataError("テストデータが空です")
        result = handler.handle_exception(error, "テストコンテキスト")
        
        # ポップアップ表示が呼ばれることを確認
        error_display_mock.assert_called_once_with("テストデータが空です")
        
        # 処理継続可能であることを確認
        assert result is True
    
    def test_gui_system_exception_handling(self):
        """GUIシステム例外ハンドリングのテスト"""
        error_display_mock = Mock()
        handler = create_gui_exception_handler(error_display_mock)
        
        # システム例外を処理
        error = NetworkError("ネットワーク接続に失敗しました")
        result = handler.handle_exception(error, "テストコンテキスト")
        
        # ポップアップ表示が呼ばれることを確認
        error_display_mock.assert_called_once_with("ネットワーク接続に失敗しました")
        
        # 処理継続不可であることを確認
        assert result is False
    
    def test_gui_unexpected_exception_handling(self):
        """GUI想定外例外ハンドリングのテスト（要件4.4）"""
        error_display_mock = Mock()
        handler = create_gui_exception_handler(error_display_mock)
        
        # 想定外例外を処理
        error = ValueError("想定外のエラー")
        result = handler.handle_exception(error, "テストコンテキスト")
        
        # 短文通知が呼ばれることを確認
        error_display_mock.assert_called_once_with("想定外のエラーが発生しました。ログファイルを確認してください。")
        
        # 処理継続不可であることを確認
        assert result is False
    
    def test_cli_business_exception_handling(self):
        """CLI業務例外ハンドリングのテスト（要件4.3）"""
        handler = create_cli_exception_handler()
        
        # 標準エラー出力をキャプチャ
        with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
            # 業務例外を処理（終了しない）
            error = EmptyDataError("テストデータが空です")
            result = handler.handle_exception(error, "テストコンテキスト", exit_on_error=False)
            
            # 標準エラー出力にメッセージが出力されることを確認
            assert "エラー: テストデータが空です" in mock_stderr.getvalue()
            
            # 処理継続可能であることを確認
            assert result is True
    
    def test_cli_business_exception_with_exit(self):
        """CLI業務例外での終了処理テスト（要件4.3）"""
        handler = create_cli_exception_handler()
        
        # sys.exitをモック
        with patch('sys.exit') as mock_exit, \
             patch('sys.stderr', new_callable=StringIO) as mock_stderr:
            
            # 業務例外を処理（終了する）
            error = EmptyDataError("テストデータが空です")
            handler.handle_exception(error, "テストコンテキスト", exit_on_error=True)
            
            # 非ゼロ終了コードで終了することを確認
            mock_exit.assert_called_once_with(2)  # 業務例外の終了コード
            
            # 標準エラー出力にメッセージが出力されることを確認
            assert "エラー: テストデータが空です" in mock_stderr.getvalue()
    
    def test_cli_system_exception_handling(self):
        """CLIシステム例外ハンドリングのテスト"""
        handler = create_cli_exception_handler()
        
        with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
            # システム例外を処理
            error = NetworkError("ネットワーク接続に失敗しました")
            result = handler.handle_exception(error, "テストコンテキスト", exit_on_error=False)
            
            # 標準エラー出力にメッセージが出力されることを確認
            assert "エラー: ネットワーク接続に失敗しました" in mock_stderr.getvalue()
            
            # 処理継続不可であることを確認
            assert result is False
    
    def test_cli_unexpected_exception_handling(self):
        """CLI想定外例外ハンドリングのテスト（要件4.4）"""
        handler = create_cli_exception_handler()
        
        with patch('sys.stderr', new_callable=StringIO) as mock_stderr:
            # 想定外例外を処理
            error = ValueError("想定外のエラー")
            result = handler.handle_exception(error, "テストコンテキスト", exit_on_error=False)
            
            # 短文通知が標準エラー出力されることを確認
            assert "エラー: 想定外のエラーが発生しました。ログファイルを確認してください。" in mock_stderr.getvalue()
            
            # 処理継続不可であることを確認
            assert result is False
    
    def test_consistent_behavior_gui_cli(self):
        """GUI・CLI一貫した挙動のテスト（要件4.5）"""
        # GUI例外ハンドラー
        gui_error_display_mock = Mock()
        gui_handler = create_gui_exception_handler(gui_error_display_mock)
        
        # CLI例外ハンドラー
        cli_handler = create_cli_exception_handler()
        
        # 同じ例外を両方で処理
        error = EmptyDataError("テストデータが空です")
        
        # GUI処理
        gui_result = gui_handler.handle_exception(error, "テストコンテキスト")
        
        # CLI処理
        with patch('sys.stderr', new_callable=StringIO):
            cli_result = cli_handler.handle_exception(error, "テストコンテキスト", exit_on_error=False)
        
        # 両方とも処理継続可能であることを確認（一貫した挙動）
        assert gui_result == cli_result == True
    
    def test_handle_exception_with_context(self):
        """コンテキスト付き例外ハンドリングのテスト"""
        handler = create_cli_exception_handler()
        
        # 正常な関数
        def normal_func(x, y):
            return x + y
        
        # 例外を発生させる関数
        def error_func():
            raise ValueError("テストエラー")
        
        # 正常ケース
        result = handle_exception_with_context(
            normal_func, "正常処理", handler, 1, 2
        )
        assert result == 3
        
        # 例外ケース
        with patch('sys.stderr', new_callable=StringIO):
            result = handle_exception_with_context(
                error_func, "エラー処理", handler
            )
            assert result is None


class TestExceptionIntegration:
    """例外ハンドリング統合テスト"""
    
    def test_logging_integration(self):
        """ログ統合のテスト"""
        # ログ出力の確認は実際のログ出力をテストする
        handler = create_cli_exception_handler()
        
        # 業務例外の処理（ログ出力は実際に行われる）
        error = EmptyDataError("テストデータが空です")
        with patch('sys.stderr', new_callable=StringIO):
            result = handler.handle_exception(error, "テストコンテキスト")
            # 処理が正常に完了することを確認
            assert result is True
        
        # 想定外例外の処理（ログ出力は実際に行われる）
        error = ValueError("想定外のエラー")
        with patch('sys.stderr', new_callable=StringIO):
            result = handler.handle_exception(error, "テストコンテキスト")
            # 処理が正常に完了することを確認
            assert result is False


if __name__ == '__main__':
    pytest.main([__file__])