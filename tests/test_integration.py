"""
統合テストの実装

データ取得からExcel出力までのエンドツーエンドテスト、
複数観測所の処理フローテスト、エラーケースの統合テスト
"""

import pytest
import pandas as pd
from unittest.mock import Mock, patch, MagicMock
from pathlib import Path
import tempfile
import shutil
from datetime import datetime

from src.wia.api import execute_data_acquisition, execute_single_station
from src.wia.types import DataRequest, StationInfo, ExcelOptions, ChartConfig
from src.wia.errors import EmptyDataError, NetworkError, ParseError, WaterInfoAcquirerError
from src.wia.exception_handler import ExceptionHandler


class TestSingleStationIntegration:
    """単一観測所の統合テスト"""
    
    def setup_method(self):
        """各テストメソッドの前に実行される初期化"""
        # テスト用の一時ディレクトリを作成
        self.temp_dir = Path(tempfile.mkdtemp())
        
    def teardown_method(self):
        """各テストメソッドの後に実行されるクリーンアップ"""
        # テスト用ディレクトリを削除
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    @patch('src.wia.api.write_timeseries_excel')
    @patch('src.wia.api.fetch_timeseries_data')
    @patch('src.wia.api.fetch_station_info')
    def test_execute_single_station_success(self, mock_fetch_info, mock_fetch_data, mock_write_excel):
        """単一観測所の正常処理テスト"""
        # モックデータ設定
        mock_station_info = StationInfo(
            code="12345",
            name="テスト観測所",
            raw_name="テスト観測所（てすとかんそくしょ）"
        )
        mock_fetch_info.return_value = mock_station_info
        
        # サンプルDataFrame作成
        mock_df = pd.DataFrame({
            'datetime': pd.date_range('2023-01-01', periods=24, freq='H'),
            'value': [1.0 + i * 0.1 for i in range(24)],
            'display_dt': pd.date_range('2023-01-01 01:00', periods=24, freq='H'),
            'sheet_year': [2023] * 24
        })
        mock_fetch_data.return_value = mock_df
        
        # Excel出力のモック
        expected_path = self.temp_dir / "test_output.xlsx"
        mock_write_excel.return_value = expected_path
        
        # 実行
        result_path = execute_single_station(
            code="12345",
            start_year=2023,
            start_month=1,
            end_year=2023,
            end_month=1,
            mode="S",
            granularity="hour",
            single_sheet=False
        )
        
        # 結果確認
        assert result_path == expected_path
        
        # 各関数が適切な引数で呼ばれることを確認
        mock_fetch_info.assert_called_once_with("12345", "S")
        mock_fetch_data.assert_called_once()
        mock_write_excel.assert_called_once()
        
        # DataRequestの内容確認
        call_args = mock_fetch_data.call_args[0][0]
        assert call_args.code == "12345"
        assert call_args.start_year == 2023
        assert call_args.mode == "S"
        assert call_args.granularity == "hour"
    
    @patch('src.wia.api.fetch_station_info')
    def test_execute_single_station_empty_data_error(self, mock_fetch_info):
        """単一観測所の空データエラーテスト"""
        mock_fetch_info.side_effect = EmptyDataError("指定期間に有効なデータが見つかりませんでした")
        
        with pytest.raises(EmptyDataError) as exc_info:
            execute_single_station(
                code="12345",
                start_year=2023,
                start_month=1,
                end_year=2023,
                end_month=1,
                mode="S"
            )
        
        assert "指定期間に有効なデータが見つかりませんでした" in str(exc_info.value)
    
    @patch('src.wia.api.fetch_station_info')
    def test_execute_single_station_network_error(self, mock_fetch_info):
        """単一観測所のネットワークエラーテスト"""
        mock_fetch_info.side_effect = NetworkError("ネットワーク接続エラーが発生しました")
        
        with pytest.raises(NetworkError) as exc_info:
            execute_single_station(
                code="12345",
                start_year=2023,
                start_month=1,
                end_year=2023,
                end_month=1,
                mode="S"
            )
        
        assert "ネットワーク接続エラーが発生しました" in str(exc_info.value)
    
    @patch('src.wia.api.fetch_station_info')
    def test_execute_single_station_unexpected_error(self, mock_fetch_info):
        """単一観測所の想定外エラーテスト"""
        mock_fetch_info.side_effect = ValueError("想定外のエラー")
        
        with pytest.raises(WaterInfoAcquirerError) as exc_info:
            execute_single_station(
                code="12345",
                start_year=2023,
                start_month=1,
                end_year=2023,
                end_month=1,
                mode="S"
            )
        
        assert "観測所コード 12345 の処理でエラーが発生しました" in str(exc_info.value)
        assert isinstance(exc_info.value.__cause__, ValueError)


class TestMultipleStationIntegration:
    """複数観測所の統合テスト"""
    
    def setup_method(self):
        """各テストメソッドの前に実行される初期化"""
        self.temp_dir = Path(tempfile.mkdtemp())
        
    def teardown_method(self):
        """各テストメソッドの後に実行されるクリーンアップ"""
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    @patch('src.wia.api.execute_single_station')
    def test_execute_data_acquisition_all_success(self, mock_execute_single):
        """全観測所成功の複数観測所処理テスト"""
        # モック設定
        expected_files = [
            self.temp_dir / "12345_test1.xlsx",
            self.temp_dir / "67890_test2.xlsx",
            self.temp_dir / "11111_test3.xlsx"
        ]
        mock_execute_single.side_effect = expected_files
        
        # 実行
        results, errors = execute_data_acquisition(
            codes=["12345", "67890", "11111"],
            start_year=2023,
            start_month=1,
            end_year=2023,
            end_month=12,
            mode="S",
            granularity="hour",
            single_sheet=False
        )
        
        # 結果確認
        assert len(results) == 3
        assert len(errors) == 0
        assert results == expected_files
        
        # 各観測所が適切に処理されることを確認
        assert mock_execute_single.call_count == 3
        
        # 最初の呼び出しの引数確認
        first_call = mock_execute_single.call_args_list[0]
        assert first_call[1]['code'] == "12345"
        assert first_call[1]['start_year'] == 2023
        assert first_call[1]['mode'] == "S"
    
    @patch('src.wia.api.execute_single_station')
    def test_execute_data_acquisition_partial_success(self, mock_execute_single):
        """一部成功・一部エラーの複数観測所処理テスト"""
        # モック設定（2番目の観測所でエラー）
        def side_effect(code, **kwargs):
            if code == "67890":
                raise EmptyDataError("指定期間に有効なデータが見つかりませんでした")
            return self.temp_dir / f"{code}_test.xlsx"
        
        mock_execute_single.side_effect = side_effect
        
        # 実行
        results, errors = execute_data_acquisition(
            codes=["12345", "67890", "11111"],
            start_year=2023,
            start_month=1,
            end_year=2023,
            end_month=12,
            mode="S"
        )
        
        # 結果確認
        assert len(results) == 2  # 成功した観測所
        assert len(errors) == 1   # エラーが発生した観測所
        
        # エラー内容確認
        error_code, error_message = errors[0]
        assert error_code == "67890"
        assert "指定期間に有効なデータが見つかりませんでした" in error_message
        
        # 成功したファイル確認
        expected_files = [
            self.temp_dir / "12345_test.xlsx",
            self.temp_dir / "11111_test.xlsx"
        ]
        assert results == expected_files
    
    @patch('src.wia.api.execute_single_station')
    def test_execute_data_acquisition_with_exception_handler(self, mock_execute_single):
        """例外ハンドラー付きの複数観測所処理テスト"""
        # モック例外ハンドラー作成
        mock_handler = Mock(spec=ExceptionHandler)
        mock_handler.handle_exception.return_value = True  # 継続可能
        
        # モック設定（2番目の観測所でエラー）
        def side_effect(code, **kwargs):
            if code == "67890":
                raise NetworkError("ネットワークエラー")
            return self.temp_dir / f"{code}_test.xlsx"
        
        mock_execute_single.side_effect = side_effect
        
        # 実行
        results, errors = execute_data_acquisition(
            codes=["12345", "67890", "11111"],
            start_year=2023,
            start_month=1,
            end_year=2023,
            end_month=12,
            mode="S",
            exception_handler=mock_handler
        )
        
        # 結果確認
        assert len(results) == 2  # 成功した観測所
        assert len(errors) == 1   # エラーが発生した観測所
        
        # 例外ハンドラーが呼ばれることを確認
        mock_handler.handle_exception.assert_called_once()
        call_args = mock_handler.handle_exception.call_args[0]
        assert isinstance(call_args[0], NetworkError)
        assert "観測所 67890 の処理" in call_args[1]
    
    @patch('src.wia.api.execute_single_station')
    def test_execute_data_acquisition_stop_on_critical_error(self, mock_execute_single):
        """継続不可能エラーでの処理中断テスト"""
        # モック例外ハンドラー作成
        mock_handler = Mock(spec=ExceptionHandler)
        mock_handler.handle_exception.return_value = False  # 継続不可能
        
        # モック設定（2番目の観測所でエラー）
        def side_effect(code, **kwargs):
            if code == "67890":
                raise NetworkError("致命的なネットワークエラー")
            return self.temp_dir / f"{code}_test.xlsx"
        
        mock_execute_single.side_effect = side_effect
        
        # 実行
        results, errors = execute_data_acquisition(
            codes=["12345", "67890", "11111"],
            start_year=2023,
            start_month=1,
            end_year=2023,
            end_month=12,
            mode="S",
            exception_handler=mock_handler
        )
        
        # 結果確認
        assert len(results) == 1  # 最初の観測所のみ成功
        assert len(errors) == 1   # エラーが発生した観測所
        
        # 3番目の観測所は処理されないことを確認
        assert mock_execute_single.call_count == 2  # 1番目と2番目のみ
    
    @patch('src.wia.api.execute_single_station')
    def test_execute_data_acquisition_different_modes(self, mock_execute_single):
        """異なるモードでの複数観測所処理テスト"""
        # 各モードでのテスト
        modes = ["S", "R", "U"]
        
        for mode in modes:
            mock_execute_single.reset_mock()
            mock_execute_single.return_value = self.temp_dir / f"test_{mode}.xlsx"
            
            results, errors = execute_data_acquisition(
                codes=["12345"],
                start_year=2023,
                start_month=1,
                end_year=2023,
                end_month=1,
                mode=mode
            )
            
            # 結果確認
            assert len(results) == 1
            assert len(errors) == 0
            
            # 適切なモードで呼ばれることを確認
            call_args = mock_execute_single.call_args[1]
            assert call_args['mode'] == mode
    
    @patch('src.wia.api.execute_single_station')
    def test_execute_data_acquisition_different_granularities(self, mock_execute_single):
        """異なる粒度での複数観測所処理テスト"""
        # 各粒度でのテスト
        granularities = ["hour", "day"]
        
        for granularity in granularities:
            mock_execute_single.reset_mock()
            mock_execute_single.return_value = self.temp_dir / f"test_{granularity}.xlsx"
            
            results, errors = execute_data_acquisition(
                codes=["12345"],
                start_year=2023,
                start_month=1,
                end_year=2023,
                end_month=1,
                mode="S",
                granularity=granularity
            )
            
            # 結果確認
            assert len(results) == 1
            assert len(errors) == 0
            
            # 適切な粒度で呼ばれることを確認
            call_args = mock_execute_single.call_args[1]
            assert call_args['granularity'] == granularity


class TestEndToEndIntegration:
    """エンドツーエンド統合テスト"""
    
    def setup_method(self):
        """各テストメソッドの前に実行される初期化"""
        self.temp_dir = Path(tempfile.mkdtemp())
        
    def teardown_method(self):
        """各テストメソッドの後に実行されるクリーンアップ"""
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    @patch('src.wia.data_source.requests.get')
    @patch('src.wia.excel_writer.pd.ExcelWriter')
    def test_end_to_end_water_level_hourly(self, mock_excel_writer, mock_requests_get):
        """水位・時間次データのエンドツーエンドテスト"""
        # HTTPレスポンスのモック設定
        mock_response = Mock()
        mock_response.encoding = 'euc_jp'
        
        # 観測所情報のHTMLレスポンス
        station_info_html = '''
        <table border="1" cellpadding="2" cellspacing="1">
            <tr><th>項目</th><th>値</th></tr>
            <tr><td>観測所名</td><td>テスト観測所（てすとかんそくしょ）</td></tr>
        </table>
        '''
        
        # 時系列データのHTMLレスポンス（24時間分）
        timeseries_html = '''
        <table>
        ''' + ''.join([
            f'<tr><td><font>{1.0 + i * 0.1:.1f}</font></td></tr>'
            for i in range(24)
        ]) + '''
        </table>
        '''
        
        # リクエストに応じて異なるレスポンスを返す
        def mock_get_side_effect(url, **kwargs):
            if 'DspWaterData.exe' in url and 'KIND=1' in url:
                # 観測所情報取得
                mock_response.text = station_info_html
            else:
                # 時系列データ取得
                mock_response.text = timeseries_html
            return mock_response
        
        mock_requests_get.side_effect = mock_get_side_effect
        
        # ExcelWriterのモック設定
        mock_writer = Mock()
        mock_excel_writer.return_value.__enter__.return_value = mock_writer
        
        # 実行
        with patch('src.wia.excel_writer.Path.exists', return_value=False):
            with patch('src.wia.excel_writer.Path.mkdir'):
                results, errors = execute_data_acquisition(
                    codes=["12345"],
                    start_year=2023,
                    start_month=1,
                    end_year=2023,
                    end_month=1,
                    mode="S",
                    granularity="hour",
                    single_sheet=False
                )
        
        # 結果確認
        assert len(results) == 1
        assert len(errors) == 0
        
        # HTTPリクエストが適切に呼ばれることを確認
        assert mock_requests_get.call_count >= 2  # 観測所情報 + 時系列データ
        
        # ExcelWriterが呼ばれることを確認
        mock_excel_writer.assert_called()
    
    @patch('src.wia.data_source.requests.get')
    def test_end_to_end_network_error_handling(self, mock_requests_get):
        """ネットワークエラーのエンドツーエンドテスト"""
        # ネットワークエラーを発生させる
        import requests
        mock_requests_get.side_effect = requests.ConnectionError("接続エラー")
        
        # 実行
        results, errors = execute_data_acquisition(
            codes=["12345"],
            start_year=2023,
            start_month=1,
            end_year=2023,
            end_month=1,
            mode="S"
        )
        
        # 結果確認
        assert len(results) == 0
        assert len(errors) == 1
        
        # エラー内容確認
        error_code, error_message = errors[0]
        assert error_code == "12345"
        assert "ネットワーク" in error_message or "接続" in error_message
    
    @patch('src.wia.data_source.requests.get')
    def test_end_to_end_empty_data_handling(self, mock_requests_get):
        """空データのエンドツーエンドテスト"""
        # 空データのHTMLレスポンス
        mock_response = Mock()
        mock_response.encoding = 'euc_jp'
        mock_response.text = '<table></table>'  # 空のテーブル
        mock_requests_get.return_value = mock_response
        
        # 実行
        results, errors = execute_data_acquisition(
            codes=["12345"],
            start_year=2023,
            start_month=1,
            end_year=2023,
            end_month=1,
            mode="S"
        )
        
        # 結果確認
        assert len(results) == 0
        assert len(errors) == 1
        
        # エラー内容確認
        error_code, error_message = errors[0]
        assert error_code == "12345"
        assert "データ" in error_message


class TestErrorCaseIntegration:
    """エラーケースの統合テスト"""
    
    @patch('src.wia.api.execute_single_station')
    def test_mixed_error_types(self, mock_execute_single):
        """異なる種類のエラーが混在する場合のテスト"""
        # 異なるエラーを発生させる
        def side_effect(code, **kwargs):
            if code == "12345":
                return Path("success.xlsx")
            elif code == "67890":
                raise EmptyDataError("データが空です")
            elif code == "11111":
                raise NetworkError("ネットワークエラー")
            elif code == "22222":
                raise ValueError("想定外エラー")
            else:
                return Path(f"{code}.xlsx")
        
        mock_execute_single.side_effect = side_effect
        
        # 実行
        results, errors = execute_data_acquisition(
            codes=["12345", "67890", "11111", "22222", "33333"],
            start_year=2023,
            start_month=1,
            end_year=2023,
            end_month=1,
            mode="S"
        )
        
        # 結果確認
        assert len(results) == 2  # 成功した観測所（12345, 33333）
        assert len(errors) == 3   # エラーが発生した観測所（67890, 11111, 22222）
        
        # エラーメッセージの確認
        error_codes = [error[0] for error in errors]
        assert "67890" in error_codes
        assert "11111" in error_codes
        assert "22222" in error_codes
    
    @patch('src.wia.api.execute_single_station')
    def test_logging_during_integration(self, mock_execute_single):
        """統合処理中のログ出力テスト"""
        mock_execute_single.return_value = Path("test.xlsx")
        
        with patch('src.wia.api.logger') as mock_logger:
            results, errors = execute_data_acquisition(
                codes=["12345", "67890"],
                start_year=2023,
                start_month=1,
                end_year=2023,
                end_month=1,
                mode="S"
            )
            
            # ログが適切に出力されることを確認
            mock_logger.info.assert_called()
            
            # 開始・完了ログの確認
            info_calls = [call[0][0] for call in mock_logger.info.call_args_list]
            assert any("データ取得開始" in call for call in info_calls)
            assert any("データ取得完了" in call for call in info_calls)


if __name__ == "__main__":
    pytest.main([__file__])