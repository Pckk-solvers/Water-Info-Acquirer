"""
Excel出力のスモークテスト

Excel存在・シート・列・チャート定義の確認テスト
既存出力との比較テスト
"""

import pytest
import pandas as pd
from pathlib import Path
from datetime import datetime
import tempfile
import os

from src.wia.excel_writer import write_timeseries_excel, generate_filename
from src.wia.types import StationInfo, DataRequest, ExcelOptions, ChartConfig
from src.wia.errors import EmptyDataError


class TestExcelWriter:
    """Excel出力のテストクラス"""
    
    def setup_method(self):
        """テストメソッド実行前の準備"""
        # テスト用データの準備
        self.station_info = StationInfo(
            code="123456789",
            name="テスト観測所",
            raw_name="テスト観測所（てすとかんそくしょ）"
        )
        
        self.request_hour = DataRequest(
            code="123456789",
            start_year=2023,
            start_month=1,
            end_year=2023,
            end_month=3,
            mode="S",
            granularity="hour"
        )
        
        self.request_day = DataRequest(
            code="123456789",
            start_year=2023,
            start_month=1,
            end_year=2023,
            end_month=3,
            mode="S",
            granularity="day"
        )
        
        # テスト用時系列データ（時間次）
        dates_hour = pd.date_range(
            start=datetime(2023, 1, 1, 0, 0),
            end=datetime(2023, 3, 31, 23, 0),
            freq='h'
        )
        self.df_hour = pd.DataFrame({
            'datetime': dates_hour,
            'value': range(len(dates_hour)),
            'display_dt': dates_hour + pd.Timedelta(hours=1),
            'sheet_year': dates_hour.year
        })
        
        # テスト用時系列データ（日次）
        dates_day = pd.date_range(
            start=datetime(2023, 1, 1),
            end=datetime(2023, 3, 31),
            freq='D'
        )
        self.df_day = pd.DataFrame({
            'datetime': dates_day,
            'value': range(len(dates_day)),
            'display_dt': dates_day,
            'sheet_year': dates_day.year
        })
    
    def teardown_method(self):
        """テストメソッド実行後のクリーンアップ"""
        # テスト用Excelファイルを削除
        test_files = [
            "123456789_テスト観測所_2023年1月-2023年3月_WH.xlsx",
            "123456789_テスト観測所_2023年1月-2023年3月_WD.xlsx"
        ]
        for file in test_files:
            if Path(file).exists():
                Path(file).unlink()
    
    def test_generate_filename_hour(self):
        """ファイル名生成テスト（時間次）"""
        filename = generate_filename(self.station_info, self.request_hour)
        expected = "123456789_テスト観測所_2023年1月-2023年3月_WH.xlsx"
        assert filename == expected
    
    def test_generate_filename_day(self):
        """ファイル名生成テスト（日次）"""
        filename = generate_filename(self.station_info, self.request_day)
        expected = "123456789_テスト観測所_2023年1月-2023年3月_WD.xlsx"
        assert filename == expected
    
    def test_empty_data_error(self):
        """空データエラーテスト"""
        empty_df = pd.DataFrame({
            'datetime': [],
            'value': [],
            'display_dt': [],
            'sheet_year': []
        })
        
        options = ExcelOptions()
        
        with pytest.raises(EmptyDataError):
            write_timeseries_excel(empty_df, self.station_info, self.request_hour, options)
    
    def test_excel_output_basic_structure(self):
        """Excel出力の基本構造テスト"""
        options = ExcelOptions(single_sheet=False, include_summary=True)
        
        # Excel出力実行
        output_path = write_timeseries_excel(
            self.df_hour, self.station_info, self.request_hour, options
        )
        
        # ファイル存在確認
        assert output_path.exists(), f"Excelファイルが作成されていません: {output_path}"
        
        # Excelファイル読み込みテスト
        excel_file = pd.ExcelFile(output_path)
        
        # 期待されるシート名
        expected_sheets = ["2023年", "summary"]
        
        # シート存在確認
        for sheet_name in expected_sheets:
            assert sheet_name in excel_file.sheet_names, f"シート '{sheet_name}' が存在しません"
        
        # 2023年シートの列構造確認
        df_2023 = pd.read_excel(output_path, sheet_name="2023年")
        expected_columns = ["datetime", "水位"]
        assert list(df_2023.columns) == expected_columns, f"列構造が期待と異なります: {list(df_2023.columns)}"
        
        # データが存在することを確認
        assert len(df_2023) > 0, "2023年シートにデータが存在しません"
        
        # summaryシートの存在確認
        df_summary = pd.read_excel(output_path, sheet_name="summary")
        assert len(df_summary) > 0, "summaryシートにデータが存在しません"
    
    def test_excel_output_single_sheet_mode(self):
        """single_sheetモードのテスト"""
        options = ExcelOptions(single_sheet=True, include_summary=True)
        
        # Excel出力実行
        output_path = write_timeseries_excel(
            self.df_hour, self.station_info, self.request_hour, options
        )
        
        # ファイル存在確認
        assert output_path.exists(), f"Excelファイルが作成されていません: {output_path}"
        
        # Excelファイル読み込みテスト
        excel_file = pd.ExcelFile(output_path)
        
        # 全期間シートの存在確認
        assert "全期間" in excel_file.sheet_names, "全期間シートが存在しません"
        
        # 全期間シートの列構造確認
        df_full = pd.read_excel(output_path, sheet_name="全期間")
        expected_columns = ["datetime", "水位"]
        assert list(df_full.columns) == expected_columns, f"全期間シートの列構造が期待と異なります: {list(df_full.columns)}"
        
        # データが存在することを確認
        assert len(df_full) > 0, "全期間シートにデータが存在しません"
    
    def test_excel_output_day_mode(self):
        """日次データモードのテスト"""
        options = ExcelOptions(single_sheet=False, include_summary=True)
        
        # Excel出力実行
        output_path = write_timeseries_excel(
            self.df_day, self.station_info, self.request_day, options
        )
        
        # ファイル存在確認
        assert output_path.exists(), f"Excelファイルが作成されていません: {output_path}"
        
        # ファイル名確認（日次データ用接尾辞）
        assert "_WD.xlsx" in str(output_path), "日次データ用のファイル接尾辞が正しくありません"
        
        # Excelファイル読み込みテスト
        excel_file = pd.ExcelFile(output_path)
        
        # 2023年シートの存在確認
        assert "2023年" in excel_file.sheet_names, "2023年シートが存在しません"
        
        # 2023年シートの列構造確認
        df_2023 = pd.read_excel(output_path, sheet_name="2023年")
        expected_columns = ["datetime", "水位"]
        assert list(df_2023.columns) == expected_columns, f"列構造が期待と異なります: {list(df_2023.columns)}"


class TestExcelWriterIntegration:
    """Excel出力の統合テスト"""
    
    def test_mode_variations(self):
        """各モード（水位・流量・雨量）のテスト"""
        station_info = StationInfo(
            code="123456789",
            name="テスト観測所",
            raw_name="テスト観測所（てすとかんそくしょ）"
        )
        
        # テスト用データ
        dates = pd.date_range(
            start=datetime(2023, 1, 1, 0, 0),
            end=datetime(2023, 1, 31, 23, 0),
            freq='h'
        )
        df = pd.DataFrame({
            'datetime': dates,
            'value': range(len(dates)),
            'display_dt': dates + pd.Timedelta(hours=1),
            'sheet_year': dates.year
        })
        
        modes = [
            ("S", "水位", "WH"),
            ("R", "流量", "QH"),
            ("U", "雨量", "RH")
        ]
        
        for mode, label, suffix in modes:
            request = DataRequest(
                code="123456789",
                start_year=2023,
                start_month=1,
                end_year=2023,
                end_month=1,
                mode=mode,
                granularity="hour"
            )
            
            options = ExcelOptions()
            
            # Excel出力実行
            output_path = write_timeseries_excel(df, station_info, request, options)
            
            try:
                # ファイル存在確認
                assert output_path.exists(), f"モード {mode} のExcelファイルが作成されていません"
                
                # ファイル名の接尾辞確認
                assert f"_{suffix}.xlsx" in str(output_path), f"モード {mode} のファイル接尾辞が正しくありません"
                
                # 列名確認
                df_sheet = pd.read_excel(output_path, sheet_name="2023年")
                expected_columns = ["datetime", label]
                assert list(df_sheet.columns) == expected_columns, f"モード {mode} の列構造が期待と異なります"
                
            finally:
                # クリーンアップ
                if output_path.exists():
                    output_path.unlink()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])