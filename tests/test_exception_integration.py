"""
例外ハンドリング統合テスト

実際のAPI呼び出しでの例外ハンドリングを検証
"""

import pytest
from unittest.mock import Mock, patch
from src.wia.api import execute_data_acquisition
from src.wia.exception_handler import create_gui_exception_handler, create_cli_exception_handler
from src.wia.errors import EmptyDataError, NetworkError


class TestExceptionIntegration:
    """例外ハンドリング統合テスト"""
    
    @patch('src.wia.api.execute_single_station')
    def test_gui_exception_handling_integration(self, mock_execute_single):
        """GUI例外ハンドリング統合テスト"""
        # エラー表示関数のモック
        error_display_mock = Mock()
        gui_handler = create_gui_exception_handler(error_display_mock)
        
        # 単一観測所処理でEmptyDataErrorを発生させる
        mock_execute_single.side_effect = EmptyDataError("指定期間にデータがありません")
        
        # API実行
        results, errors = execute_data_acquisition(
            codes=["1234"],
            start_year=2023,
            start_month=1,
            end_year=2023,
            end_month=12,
            mode="S",
            granularity="hour",
            single_sheet=False,
            exception_handler=gui_handler
        )
        
        # 結果確認
        assert len(results) == 0  # 成功したファイルなし
        assert len(errors) == 1   # エラーが1件
        assert "1234" in errors[0][0]  # 観測所コードが含まれる
        assert "指定期間にデータがありません" in errors[0][1]  # エラーメッセージが含まれる
    
    @patch('src.wia.api.execute_single_station')
    def test_cli_exception_handling_integration(self, mock_execute_single):
        """CLI例外ハンドリング統合テスト"""
        cli_handler = create_cli_exception_handler()
        
        # 単一観測所処理でNetworkErrorを発生させる
        mock_execute_single.side_effect = NetworkError("ネットワーク接続エラー")
        
        # API実行
        results, errors = execute_data_acquisition(
            codes=["5678"],
            start_year=2023,
            start_month=1,
            end_year=2023,
            end_month=12,
            mode="R",
            granularity="day",
            single_sheet=True,
            exception_handler=cli_handler
        )
        
        # 結果確認
        assert len(results) == 0  # 成功したファイルなし
        assert len(errors) == 1   # エラーが1件
        assert "5678" in errors[0][0]  # 観測所コードが含まれる
        assert "ネットワーク接続エラー" in errors[0][1]  # エラーメッセージが含まれる
    
    @patch('src.wia.api.execute_single_station')
    def test_unexpected_exception_handling(self, mock_execute_single):
        """想定外例外ハンドリングテスト"""
        cli_handler = create_cli_exception_handler()
        
        # 想定外例外を発生させる
        mock_execute_single.side_effect = ValueError("想定外のエラー")
        
        # API実行
        results, errors = execute_data_acquisition(
            codes=["9999"],
            start_year=2023,
            start_month=1,
            end_year=2023,
            end_month=12,
            mode="U",
            granularity="hour",
            single_sheet=False,
            exception_handler=cli_handler
        )
        
        # 結果確認
        assert len(results) == 0  # 成功したファイルなし
        assert len(errors) == 1   # エラーが1件
        assert "9999" in errors[0][0]  # 観測所コードが含まれる
        # 想定外例外は短文通知になる
        assert "想定外のエラーが発生しました" in errors[0][1]


if __name__ == '__main__':
    pytest.main([__file__])