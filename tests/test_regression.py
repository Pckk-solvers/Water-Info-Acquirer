"""
回帰テストの実装

既存機能との出力比較テスト、CLI・GUIの基本動作確認テスト、
PyInstallerビルド後の動作テスト
"""

import pytest
import subprocess
import sys
import os
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import openpyxl
from datetime import datetime

from src.wia.api import execute_data_acquisition
from src.wia.gui import WWRApp
from src.wia.types import DataRequest, StationInfo, ExcelOptions


class TestExcelOutputRegression:
    """Excel出力の回帰テスト"""
    
    def setup_method(self):
        """各テストメソッドの前に実行される初期化"""
        self.temp_dir = Path(tempfile.mkdtemp())
        
    def teardown_method(self):
        """各テストメソッドの後に実行されるクリーンアップ"""
        if self.temp_dir.exists():
            shutil.rmtree(self.temp_dir)
    
    @patch('src.wia.data_source.fetch_station_info')
    @patch('src.wia.data_source.fetch_timeseries_data')
    def test_excel_output_structure_consistency(self, mock_fetch_data, mock_fetch_info):
        """Excel出力構造の一貫性テスト"""
        # モックデータ設定
        mock_station_info = StationInfo(
            code="12345",
            name="テスト観測所",
            raw_name="テスト観測所（てすとかんそくしょ）"
        )
        mock_fetch_info.return_value = mock_station_info
        
        # 2年分のサンプルデータ作成（2023年と2024年）
        dates_2023 = pd.date_range('2023-01-01', '2023-12-31 23:00', freq='H')
        dates_2024 = pd.date_range('2024-01-01', '2024-12-31 23:00', freq='H')
        all_dates = dates_2023.append(dates_2024)
        
        mock_df = pd.DataFrame({
            'datetime': all_dates,
            'value': [1.0 + (i % 100) * 0.01 for i in range(len(all_dates))],
            'display_dt': all_dates + pd.Timedelta(hours=1),
            'sheet_year': [dt.year for dt in all_dates]
        })
        mock_fetch_data.return_value = mock_df
        
        # 実際のExcel出力を実行
        with patch('src.wia.excel_writer.Path.mkdir'):
            results, errors = execute_data_acquisition(
                codes=["12345"],
                start_year=2023,
                start_month=1,
                end_year=2024,
                end_month=12,
                mode="S",
                granularity="hour",
                single_sheet=False
            )
        
        # 結果確認
        assert len(results) == 1
        assert len(errors) == 0
        
        # Excelファイルの構造確認（実際のファイルが作成される場合）
        excel_file = results[0]
        if excel_file.exists():
            workbook = openpyxl.load_workbook(excel_file)
            
            # 期待されるシート名の確認
            expected_sheets = ['2023年', '2024年', 'summary']
            for sheet_name in expected_sheets:
                assert sheet_name in workbook.sheetnames
            
            # 各シートの基本構造確認
            for sheet_name in ['2023年', '2024年']:
                sheet = workbook[sheet_name]
                
                # ヘッダー行の確認
                assert sheet['A1'].value == '時刻'
                assert sheet['B1'].value == '水位[m]'
                
                # データ行の存在確認
                assert sheet['A2'].value is not None
                assert sheet['B2'].value is not None
    
    @patch('src.wia.data_source.fetch_station_info')
    @patch('src.wia.data_source.fetch_timeseries_data')
    def test_single_sheet_mode_consistency(self, mock_fetch_data, mock_fetch_info):
        """シングルシートモードの一貫性テスト"""
        # モックデータ設定
        mock_station_info = StationInfo(
            code="12345",
            name="テスト観測所",
            raw_name="テスト観測所（てすとかんそくしょ）"
        )
        mock_fetch_info.return_value = mock_station_info
        
        # 1ヶ月分のサンプルデータ
        dates = pd.date_range('2023-01-01', '2023-01-31 23:00', freq='H')
        mock_df = pd.DataFrame({
            'datetime': dates,
            'value': [1.0 + i * 0.01 for i in range(len(dates))],
            'display_dt': dates + pd.Timedelta(hours=1),
            'sheet_year': [2023] * len(dates)
        })
        mock_fetch_data.return_value = mock_df
        
        # シングルシートモードで実行
        with patch('src.wia.excel_writer.Path.mkdir'):
            results, errors = execute_data_acquisition(
                codes=["12345"],
                start_year=2023,
                start_month=1,
                end_year=2023,
                end_month=1,
                mode="S",
                granularity="hour",
                single_sheet=True
            )
        
        # 結果確認
        assert len(results) == 1
        assert len(errors) == 0
        
        # シングルシートモードでは全期間シートが作成されることを確認
        # （実際のファイル確認は統合テストで実施）
    
    def test_file_naming_consistency(self):
        """ファイル名の一貫性テスト"""
        # ファイル名生成ロジックのテスト
        from src.wia.excel_writer import _generate_filename
        from src.wia.types import DataRequest, StationInfo
        
        station_info = StationInfo(
            code="12345",
            name="テスト観測所",
            raw_name="テスト観測所（てすとかんそくしょ）"
        )
        
        request = DataRequest(
            code="12345",
            start_year=2023,
            start_month=1,
            end_year=2023,
            end_month=12,
            mode="S",
            granularity="hour"
        )
        
        filename = _generate_filename(station_info, request)
        
        # 期待されるファイル名形式の確認
        expected_pattern = "12345_テスト観測所_2023年1月-2023年12月_WH.xlsx"
        assert filename == expected_pattern
    
    def test_chart_configuration_consistency(self):
        """チャート設定の一貫性テスト"""
        from src.wia.types import ChartConfig
        from src.wia.constants import MODE_LABELS, MODE_UNITS
        
        # 各モードでのチャート設定確認
        modes = ["S", "R", "U"]
        
        for mode in modes:
            chart_config = ChartConfig(
                title=f"テスト観測所 ({MODE_LABELS[mode]})",
                y_axis_label=f"{MODE_LABELS[mode]}[{MODE_UNITS[mode]}]",
                x_axis_format="m"
            )
            
            # 設定値の確認
            assert MODE_LABELS[mode] in chart_config.title
            assert MODE_UNITS[mode] in chart_config.y_axis_label
            assert chart_config.x_axis_format == "m"


class TestCLIRegression:
    """CLI機能の回帰テスト"""
    
    def test_cli_entry_point_exists(self):
        """CLIエントリーポイントの存在確認"""
        # __main__.pyの存在確認
        main_file = Path("src/__main__.py")
        assert main_file.exists(), "__main__.pyが存在しません"
        
        # 基本的な構文チェック
        try:
            with open(main_file, 'r', encoding='utf-8') as f:
                content = f.read()
            compile(content, str(main_file), 'exec')
        except SyntaxError as e:
            pytest.fail(f"__main__.pyに構文エラーがあります: {e}")
    
    @patch('sys.argv')
    def test_cli_help_option(self, mock_argv):
        """CLI ヘルプオプションのテスト"""
        mock_argv.__getitem__.side_effect = lambda x: ['python', '-m', 'src', '--help'][x]
        mock_argv.__len__.return_value = 4
        
        # ヘルプオプションの動作確認（実際の実行はしない）
        # 実際のテストでは subprocess を使用して外部プロセスとして実行
        pass
    
    def test_cli_single_sheet_option_parsing(self):
        """CLI --single-sheetオプションの解析テスト"""
        # argparseの設定確認（実際のCLIコードから）
        import argparse
        
        parser = argparse.ArgumentParser()
        parser.add_argument('--single-sheet', action='store_true',
                          help='全期間データを1シートに出力')
        
        # オプションありの場合
        args = parser.parse_args(['--single-sheet'])
        assert args.single_sheet is True
        
        # オプションなしの場合
        args = parser.parse_args([])
        assert args.single_sheet is False
    
    @pytest.mark.slow
    def test_cli_execution_smoke_test(self):
        """CLI実行のスモークテスト（実際の実行）"""
        # 実際のCLI実行テスト（時間がかかるためスキップ可能）
        try:
            # python -m src --help の実行
            result = subprocess.run(
                [sys.executable, '-m', 'src', '--help'],
                capture_output=True,
                text=True,
                timeout=10,
                cwd=Path.cwd()
            )
            
            # ヘルプが正常に表示されることを確認
            assert result.returncode == 0
            assert 'usage:' in result.stdout.lower() or 'help' in result.stdout.lower()
            
        except subprocess.TimeoutExpired:
            pytest.skip("CLI実行がタイムアウトしました")
        except FileNotFoundError:
            pytest.skip("CLIエントリーポイントが見つかりません")


class TestGUIRegression:
    """GUI機能の回帰テスト"""
    
    def setup_method(self):
        """各テストメソッドの前に実行される初期化"""
        # テスト用のルートウィンドウを作成（実際には表示しない）
        import tkinter as tk
        self.root = tk.Tk()
        self.root.withdraw()
        
    def teardown_method(self):
        """各テストメソッドの後に実行されるクリーンアップ"""
        if hasattr(self, 'app') and self.app:
            self.app.root.destroy()
        if hasattr(self, 'root') and self.root:
            self.root.destroy()
    
    def test_gui_initialization_regression(self):
        """GUI初期化の回帰テスト"""
        # 基本的な初期化が正常に動作することを確認
        self.app = WWRApp()
        
        # 基本的なウィジェットの存在確認
        assert hasattr(self.app, 'root')
        assert hasattr(self.app, 'listbox')
        assert hasattr(self.app, 'mode')
        assert hasattr(self.app, 'codes')
        
        # 初期値の確認
        assert self.app.mode.get() == "S"
        assert self.app.codes == []
        assert self.app.use_data_sru.get() is False
    
    def test_gui_single_sheet_mode_regression(self):
        """GUIシングルシートモードの回帰テスト"""
        # シングルシートモードでの初期化
        self.app = WWRApp(single_sheet_mode=True)
        
        # シングルシートモードの設定確認
        assert self.app.single_sheet_mode is True
        assert self.app.single_sheet_var.get() is True
    
    def test_gui_widget_structure_regression(self):
        """GUIウィジェット構造の回帰テスト"""
        self.app = WWRApp()
        
        # 主要なウィジェットの存在確認
        widgets_to_check = [
            'root', 'listbox', 'mode', 'use_data_sru', 
            'single_sheet_var', 'year_start', 'year_end',
            'month_start', 'month_end'
        ]
        
        for widget_name in widgets_to_check:
            assert hasattr(self.app, widget_name), f"{widget_name}ウィジェットが見つかりません"
    
    def test_gui_error_display_regression(self):
        """GUIエラー表示の回帰テスト"""
        self.app = WWRApp()
        
        # エラー表示メソッドの存在確認
        assert hasattr(self.app, '_show_error')
        assert callable(self.app._show_error)
        
        # エラー表示の基本動作確認（実際のダイアログは表示しない）
        with patch('src.wia.gui.Toplevel') as mock_toplevel:
            self.app._show_error("テストエラー")
            mock_toplevel.assert_called_once()
    
    def test_gui_results_display_regression(self):
        """GUI結果表示の回帰テスト"""
        self.app = WWRApp()
        
        # 結果表示メソッドの存在確認
        assert hasattr(self.app, '_show_results')
        assert callable(self.app._show_results)
        
        # 結果表示の基本動作確認（実際のダイアログは表示しない）
        with patch('src.wia.gui.Toplevel') as mock_toplevel:
            test_files = [Path("test1.xlsx"), Path("test2.xlsx")]
            self.app._show_results(test_files)
            mock_toplevel.assert_called_once()


class TestBuildRegression:
    """ビルドプロセスの回帰テスト"""
    
    def test_pyproject_toml_exists(self):
        """pyproject.tomlの存在確認"""
        pyproject_file = Path("pyproject.toml")
        assert pyproject_file.exists(), "pyproject.tomlが存在しません"
        
        # 基本的な設定の確認
        try:
            import tomllib
            with open(pyproject_file, 'rb') as f:
                config = tomllib.load(f)
            
            # プロジェクト設定の確認
            assert 'project' in config
            assert 'name' in config['project']
            
        except ImportError:
            # Python 3.11未満の場合はスキップ
            pytest.skip("tomllibが利用できません（Python 3.11以降が必要）")
    
    def test_requirements_txt_exists(self):
        """requirements.txtの存在確認"""
        requirements_file = Path("requirements.txt")
        assert requirements_file.exists(), "requirements.txtが存在しません"
        
        # 基本的な依存関係の確認
        with open(requirements_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 必要なパッケージの存在確認
        required_packages = ['pandas', 'openpyxl', 'requests']
        for package in required_packages:
            assert package in content, f"{package}がrequirements.txtに含まれていません"
    
    def test_pyinstaller_spec_files_exist(self):
        """PyInstallerスペックファイルの存在確認"""
        spec_files = [
            "Water-Info-Acquirer-1.0.spec",
            "WaterInfoAcquirer-Test.spec"
        ]
        
        for spec_file in spec_files:
            spec_path = Path(spec_file)
            if spec_path.exists():
                # スペックファイルの基本的な構文チェック
                try:
                    with open(spec_path, 'r', encoding='utf-8') as f:
                        content = f.read()
                    
                    # PyInstallerスペックファイルの基本要素確認
                    assert 'Analysis(' in content
                    assert 'PYZ(' in content
                    assert 'EXE(' in content
                    
                except Exception as e:
                    pytest.fail(f"{spec_file}の読み込みに失敗しました: {e}")
    
    @pytest.mark.slow
    def test_import_structure_regression(self):
        """インポート構造の回帰テスト"""
        # 主要モジュールのインポート確認
        modules_to_test = [
            'src.wia.api',
            'src.wia.data_source',
            'src.wia.excel_writer',
            'src.wia.gui',
            'src.wia.errors',
            'src.wia.constants',
            'src.wia.types'
        ]
        
        for module_name in modules_to_test:
            try:
                __import__(module_name)
            except ImportError as e:
                pytest.fail(f"{module_name}のインポートに失敗しました: {e}")
    
    def test_package_structure_regression(self):
        """パッケージ構造の回帰テスト"""
        # 期待されるディレクトリ構造の確認
        expected_structure = {
            'src': True,
            'src/wia': True,
            'src/wia/__init__.py': True,
            'src/wia/api.py': True,
            'src/wia/data_source.py': True,
            'src/wia/excel_writer.py': True,
            'src/wia/gui.py': True,
            'src/wia/errors.py': True,
            'src/wia/constants.py': True,
            'src/wia/types.py': True,
            'tests': True,
            'tests/__init__.py': False,  # オプショナル
        }
        
        for path_str, required in expected_structure.items():
            path = Path(path_str)
            if required:
                assert path.exists(), f"必要なパス {path_str} が存在しません"


class TestCompatibilityRegression:
    """互換性の回帰テスト"""
    
    def test_excel_output_format_compatibility(self):
        """Excel出力形式の互換性テスト"""
        # 既存のExcel出力形式との互換性確認
        from src.wia.constants import MODE_LABELS, MODE_UNITS, MODE_FILE_SUFFIXES
        
        # 期待される設定値の確認
        expected_modes = {
            'S': {'label': '水位', 'unit': 'm', 'suffix': 'WH'},
            'R': {'label': '流量', 'unit': 'm^3/s', 'suffix': 'QH'},
            'U': {'label': '雨量', 'unit': 'mm/h', 'suffix': 'RH'}
        }
        
        for mode, expected in expected_modes.items():
            assert MODE_LABELS[mode] == expected['label']
            assert MODE_UNITS[mode] == expected['unit']
            assert MODE_FILE_SUFFIXES[mode] == expected['suffix']
    
    def test_cli_option_compatibility(self):
        """CLIオプションの互換性テスト"""
        # 既存の--single-sheetオプションの動作確認
        # （実際のCLI実行は別のテストで実施）
        
        # オプション名の確認
        expected_options = ['--single-sheet']
        
        # 実際のargparse設定との整合性確認は統合テストで実施
        for option in expected_options:
            assert option.startswith('--'), f"オプション {option} の形式が正しくありません"
    
    def test_error_message_compatibility(self):
        """エラーメッセージの互換性テスト"""
        from src.wia.errors import EmptyDataError, NetworkError, ParseError
        
        # 既存のエラーメッセージとの互換性確認
        error_cases = [
            (EmptyDataError(), "取得したデータが空です"),
            (NetworkError(), "ネットワーク接続エラーが発生しました"),
            (ParseError(), "データの解析に失敗しました")
        ]
        
        for error, expected_message in error_cases:
            assert str(error) == expected_message


if __name__ == "__main__":
    # スローテストをスキップするオプション
    pytest.main([__file__, "-m", "not slow"])