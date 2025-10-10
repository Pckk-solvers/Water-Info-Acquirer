"""
統合APIのテスト

既存機能との出力比較テスト
"""

import pytest
from unittest.mock import patch, Mock
import pandas as pd
from datetime import datetime
from pathlib import Path

from src.wia.api import execute_single_station, execute_data_acquisition
from src.wia.errors import EmptyDataError, WaterInfoAcquirerError


class TestAPIIntegration:
    """統合APIのテストクラス"""
    
    def setup_method(self):
        """テストメソッド実行前の準備"""
        # モックデータの準備
        self.mock_station_info = Mock()
        self.mock_station_info.code = "123456789"
        self.mock_station_info.name = "テスト観測所"
        self.mock_station_info.raw_name = "テスト観測所（てすとかんそくしょ）"
        
        # モック時系列データ
        dates = pd.date_range(
            start=datetime(2023, 1, 1, 0, 0),
            end=datetime(2023, 1, 31, 23, 0),
            freq='h'
        )
        self.mock_df = pd.DataFrame({
            'datetime': dates,
            'value': range(len(dates)),
            'display_dt': dates + pd.Timedelta(hours=1),
            'sheet_year': dates.year
        })
    
    def teardown_method(self):
        """テストメソッド実行後のクリーンアップ"""
        # テスト用Excelファイルを削除
        test_files = Path(".").glob("123456789_テスト観測所_*.xlsx")
        for file in test_files:
            if file.exists():
                file.unlink()
    
    @patch('src.wia.api.write_timeseries_excel')
    @patch('src.wia.api.fetch_timeseries_data')
    @patch('src.wia.api.fetch_station_info')
    def test_execute_single_station_success(self, mock_fetch_station, mock_fetch_data, mock_write_excel):
        """単一観測所実行の成功テスト"""
        # モックの設定
        mock_fetch_station.return_value = self.mock_station_info
        mock_fetch_data.return_value = self.mock_df
        mock_write_excel.return_value = Path("test_output.xlsx")
        
        # 実行
        result = execute_single_station(
            code="123456789",
            start_year=2023,
            start_month=1,
            end_year=2023,
            end_month=1,
            mode="S",
            granularity="hour",
            single_sheet=False
        )
        
        # 結果確認
        assert result == Path("test_output.xlsx")
        
        # モック呼び出し確認
        mock_fetch_station.assert_called_once_with("123456789", "S")
        mock_fetch_data.assert_called_once()
        mock_write_excel.assert_called_once()
    
    @patch('src.wia.api.fetch_timeseries_data')
    @patch('src.wia.api.fetch_station_info')
    def test_execute_single_station_empty_data_error(self, mock_fetch_station, mock_fetch_data):
        """空データエラーのテスト"""
        # モックの設定
        mock_fetch_station.return_value = self.mock_station_info
        mock_fetch_data.side_effect = EmptyDataError("テストエラー")
        
        # 実行と例外確認
        with pytest.raises(EmptyDataError):
            execute_single_station(
                code="123456789",
                start_year=2023,
                start_month=1,
                end_year=2023,
                end_month=1,
                mode="S"
            )
    
    @patch('src.wia.api.execute_single_station')
    def test_execute_data_acquisition_multiple_stations(self, mock_execute_single):
        """複数観測所実行のテスト"""
        # モックの設定
        mock_execute_single.side_effect = [
            Path("output1.xlsx"),
            Path("output2.xlsx"),
            Path("output3.xlsx")
        ]
        
        # 実行
        results = execute_data_acquisition(
            codes=["123456789", "987654321", "555666777"],
            start_year=2023,
            start_month=1,
            end_year=2023,
            end_month=1,
            mode="S"
        )
        
        # 結果確認
        assert len(results) == 3
        assert results[0] == Path("output1.xlsx")
        assert results[1] == Path("output2.xlsx")
        assert results[2] == Path("output3.xlsx")
        
        # モック呼び出し回数確認
        assert mock_execute_single.call_count == 3
    
    @patch('src.wia.api.execute_single_station')
    def test_execute_data_acquisition_with_empty_data_error(self, mock_execute_single):
        """空データエラーを含む複数観測所実行のテスト"""
        # モックの設定（2番目の観測所で空データエラー）
        mock_execute_single.side_effect = [
            Path("output1.xlsx"),
            EmptyDataError("観測所2で空データエラー"),
            Path("output3.xlsx")
        ]
        
        # 実行と例外確認（最初のEmptyDataErrorで停止）
        with pytest.raises(EmptyDataError):
            execute_data_acquisition(
                codes=["123456789", "987654321", "555666777"],
                start_year=2023,
                start_month=1,
                end_year=2023,
                end_month=1,
                mode="S"
            )


class TestBackwardCompatibility:
    """既存機能との互換性テスト"""
    
    def teardown_method(self):
        """テストメソッド実行後のクリーンアップ"""
        # テスト用Excelファイルを削除
        test_files = Path(".").glob("*_テスト観測所_*.xlsx")
        for file in test_files:
            if file.exists():
                file.unlink()
    
    @patch('src.wia.api.execute_single_station')
    def test_process_data_for_code_compatibility(self, mock_execute_single):
        """process_data_for_code関数の互換性テスト"""
        from src.main_datetime import process_data_for_code
        
        # モックの設定
        mock_execute_single.return_value = Path("test_output.xlsx")
        
        # 実行（既存の関数シグネチャ）
        result = process_data_for_code(
            code="123456789",
            Y1="2023",
            Y2="2023", 
            M1="1月",
            M2="3月",
            mode_type="S",
            single_sheet=False
        )
        
        # 結果確認
        assert result == "test_output.xlsx"
        
        # モック呼び出し確認
        mock_execute_single.assert_called_once_with(
            code="123456789",
            start_year=2023,
            start_month=1,
            end_year=2023,
            end_month=3,
            mode="S",
            granularity="hour",
            single_sheet=False
        )
    
    @patch('src.wia.api.execute_single_station')
    def test_process_period_date_display_for_code_compatibility(self, mock_execute_single):
        """process_period_date_display_for_code関数の互換性テスト"""
        from src.datemode import process_period_date_display_for_code
        
        # モックの設定
        mock_execute_single.return_value = Path("test_output.xlsx")
        
        # 実行（既存の関数シグネチャ）
        result = process_period_date_display_for_code(
            code="123456789",
            Y1="2023",
            Y2="2023",
            M1="1月", 
            M2="3月",
            mode_type="S",
            single_sheet=True
        )
        
        # 結果確認
        assert result == "test_output.xlsx"
        
        # モック呼び出し確認
        mock_execute_single.assert_called_once_with(
            code="123456789",
            start_year=2023,
            start_month=1,
            end_year=2023,
            end_month=3,
            mode="S",
            granularity="day",
            single_sheet=True
        )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])