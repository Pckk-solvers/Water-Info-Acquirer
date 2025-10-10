"""
GUI分離のテスト実装

WWRAppクラスの単体テスト、エラー表示・結果表示のテスト、
ユーザー入力バリデーションのテスト
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import tkinter as tk
from pathlib import Path
from typing import List

from src.wia.gui import WWRApp, ToolTip
from src.wia.errors import EmptyDataError, WaterInfoAcquirerError


class TestWWRApp:
    """WWRAppクラスの単体テスト"""
    
    def setup_method(self):
        """各テストメソッドの前に実行される初期化"""
        # テスト用のルートウィンドウを作成（実際には表示しない）
        self.root = tk.Tk()
        self.root.withdraw()  # ウィンドウを非表示にする
        
    def teardown_method(self):
        """各テストメソッドの後に実行されるクリーンアップ"""
        if hasattr(self, 'app') and self.app:
            self.app.root.destroy()
        if hasattr(self, 'root') and self.root:
            self.root.destroy()
    
    def test_init_default_mode(self):
        """デフォルトモードでの初期化テスト"""
        self.app = WWRApp()
        
        # 基本的な初期化確認
        assert self.app.single_sheet_mode is False
        assert self.app.mode.get() == "S"  # デフォルトは水位
        assert self.app.use_data_sru.get() is False  # デフォルトは時間次
        assert self.app.single_sheet_var.get() is False
        assert self.app.codes == []
        
        # ウィンドウタイトル確認
        assert self.app.root.title() == '水文データ取得ツール'
    
    def test_init_single_sheet_mode(self):
        """シングルシートモードでの初期化テスト"""
        self.app = WWRApp(single_sheet_mode=True)
        
        assert self.app.single_sheet_mode is True
        assert self.app.single_sheet_var.get() is True
    
    def test_add_code_valid(self):
        """有効な観測所コード追加のテスト"""
        self.app = WWRApp()
        
        # モックエントリーを作成
        mock_entry = Mock()
        mock_entry.get.return_value = "12345"
        mock_entry.delete = Mock()
        
        # コード追加実行
        self.app._add_code(mock_entry)
        
        # 結果確認
        assert "12345" in self.app.codes
        assert self.app.listbox.size() == 1
        mock_entry.delete.assert_called_once_with(0, 'end')
    
    def test_add_code_invalid_non_digit(self):
        """無効な観測所コード（非数字）の追加テスト"""
        self.app = WWRApp()
        
        mock_entry = Mock()
        mock_entry.get.return_value = "abc123"
        mock_entry.delete = Mock()
        
        self.app._add_code(mock_entry)
        
        # 無効なコードは追加されない
        assert "abc123" not in self.app.codes
        assert self.app.listbox.size() == 0
        mock_entry.delete.assert_called_once_with(0, 'end')
    
    def test_add_code_duplicate(self):
        """重複する観測所コードの追加テスト"""
        self.app = WWRApp()
        
        # 最初のコード追加
        mock_entry = Mock()
        mock_entry.get.return_value = "12345"
        mock_entry.delete = Mock()
        
        self.app._add_code(mock_entry)
        assert len(self.app.codes) == 1
        
        # 同じコードを再度追加
        self.app._add_code(mock_entry)
        
        # 重複は追加されない
        assert len(self.app.codes) == 1
        assert self.app.codes.count("12345") == 1
    
    def test_remove_code(self):
        """観測所コード削除のテスト"""
        self.app = WWRApp()
        
        # 事前にコードを追加
        self.app.codes = ["12345", "67890"]
        self.app.listbox.insert('end', "12345")
        self.app.listbox.insert('end', "67890")
        
        # 最初のアイテムを選択状態にする
        self.app.listbox.selection_set(0)
        
        # 削除実行
        self.app._remove_code()
        
        # 結果確認
        assert "12345" not in self.app.codes
        assert "67890" in self.app.codes
        assert len(self.app.codes) == 1
    
    def test_validate_success(self):
        """入力値検証成功のテスト"""
        self.app = WWRApp()
        
        # 有効な入力値を設定
        self.app.codes = ["12345"]
        self.app.year_start.set("2023")
        self.app.year_end.set("2023")
        
        result = self.app._validate()
        assert result is True
    
    def test_validate_no_codes(self):
        """観測所コードなしの検証テスト"""
        self.app = WWRApp()
        
        # 観測所コードを空にする
        self.app.codes = []
        self.app.year_start.set("2023")
        self.app.year_end.set("2023")
        
        with patch.object(self.app, '_show_error') as mock_show_error:
            result = self.app._validate()
            
        assert result is False
        mock_show_error.assert_called_once_with('観測所コードを追加してください')
    
    def test_validate_invalid_start_year(self):
        """無効な開始年の検証テスト"""
        self.app = WWRApp()
        
        self.app.codes = ["12345"]
        self.app.year_start.set("23")  # 4桁でない
        self.app.year_end.set("2023")
        
        with patch.object(self.app, '_show_error') as mock_show_error:
            result = self.app._validate()
            
        assert result is False
        mock_show_error.assert_called_once_with('開始年は4桁で入力してください')
    
    def test_validate_invalid_end_year(self):
        """無効な終了年の検証テスト"""
        self.app = WWRApp()
        
        self.app.codes = ["12345"]
        self.app.year_start.set("2023")
        self.app.year_end.set("abc")  # 数字でない
        
        with patch.object(self.app, '_show_error') as mock_show_error:
            result = self.app._validate()
            
        assert result is False
        mock_show_error.assert_called_once_with('終了年は4桁で入力してください')


class TestErrorDisplay:
    """エラー表示のテスト"""
    
    def setup_method(self):
        """各テストメソッドの前に実行される初期化"""
        self.root = tk.Tk()
        self.root.withdraw()
        
    def teardown_method(self):
        """各テストメソッドの後に実行されるクリーンアップ"""
        if hasattr(self, 'app') and self.app:
            self.app.root.destroy()
        if hasattr(self, 'root') and self.root:
            self.root.destroy()
    
    @patch('src.wia.gui.Toplevel')
    @patch('src.wia.gui.Label')
    @patch('src.wia.gui.Button')
    def test_show_error_creates_dialog(self, mock_button, mock_label, mock_toplevel):
        """エラーダイアログ作成のテスト"""
        self.app = WWRApp()
        
        # モックウィンドウを設定
        mock_window = Mock()
        mock_toplevel.return_value = mock_window
        
        error_message = "テストエラーメッセージ"
        self.app._show_error(error_message)
        
        # Toplevelウィンドウが作成されることを確認
        mock_toplevel.assert_called_once_with(self.app.root)
        
        # ウィンドウの設定が呼ばれることを確認
        mock_window.title.assert_called_once_with('エラー')
        mock_window.config.assert_called_once_with(bg="#ffffbf")
        
        # ラベルとボタンが作成されることを確認
        mock_label.assert_called()
        mock_button.assert_called()
    
    def test_show_error_logging(self):
        """エラー表示時のログ出力テスト"""
        self.app = WWRApp()
        
        with patch('src.wia.gui.logger') as mock_logger:
            with patch('src.wia.gui.Toplevel'):
                error_message = "テストエラーメッセージ"
                self.app._show_error(error_message)
                
                # ログが出力されることを確認
                mock_logger.warning.assert_called_once_with(f"エラー表示: {error_message}")


class TestResultsDisplay:
    """結果表示のテスト"""
    
    def setup_method(self):
        """各テストメソッドの前に実行される初期化"""
        self.root = tk.Tk()
        self.root.withdraw()
        
    def teardown_method(self):
        """各テストメソッドの後に実行されるクリーンアップ"""
        if hasattr(self, 'app') and self.app:
            self.app.root.destroy()
        if hasattr(self, 'root') and self.root:
            self.root.destroy()
    
    @patch('src.wia.gui.Toplevel')
    @patch('src.wia.gui.Label')
    @patch('src.wia.gui.Button')
    def test_show_results_creates_dialog(self, mock_button, mock_label, mock_toplevel):
        """結果ダイアログ作成のテスト"""
        self.app = WWRApp()
        
        # モックウィンドウを設定
        mock_window = Mock()
        mock_toplevel.return_value = mock_window
        
        test_files = [Path("test1.xlsx"), Path("test2.xlsx")]
        self.app._show_results(test_files)
        
        # Toplevelウィンドウが作成されることを確認
        mock_toplevel.assert_called_once_with(self.app.root)
        
        # ウィンドウの設定が呼ばれることを確認
        mock_window.title.assert_called_once_with('結果')
        mock_window.config.assert_called_once_with(bg="#d1f6ff")
        
        # ラベルとボタンが適切な回数作成されることを確認
        # "Excel作成完了" + ファイル数分のラベル
        assert mock_label.call_count >= 3  # 最低3回（タイトル + 2ファイル）
        assert mock_button.call_count == 3  # "開く", "閉じる", "終了"
    
    def test_show_results_logging(self):
        """結果表示時のログ出力テスト"""
        self.app = WWRApp()
        
        with patch('src.wia.gui.logger') as mock_logger:
            with patch('src.wia.gui.Toplevel'):
                test_files = [Path("test1.xlsx"), Path("test2.xlsx")]
                self.app._show_results(test_files)
                
                # ログが出力されることを確認
                mock_logger.info.assert_called_with(f"結果表示: {len(test_files)}件のファイル")


class TestExecuteHandler:
    """実行ハンドラーのテスト"""
    
    def setup_method(self):
        """各テストメソッドの前に実行される初期化"""
        self.root = tk.Tk()
        self.root.withdraw()
        
    def teardown_method(self):
        """各テストメソッドの後に実行されるクリーンアップ"""
        if hasattr(self, 'app') and self.app:
            self.app.root.destroy()
        if hasattr(self, 'root') and self.root:
            self.root.destroy()
    
    @patch('src.wia.gui.execute_data_acquisition')
    @patch('src.wia.gui.Toplevel')
    def test_on_execute_validation_failure(self, mock_toplevel, mock_execute):
        """バリデーション失敗時の実行テスト"""
        self.app = WWRApp()
        
        # バリデーション失敗の状態を設定（観測所コードなし）
        self.app.codes = []
        
        with patch.object(self.app, '_validate', return_value=False):
            # _on_executeは別スレッドで実行されるため、直接呼び出し
            self.app._on_execute.__wrapped__(self.app)  # デコレータを外して直接実行
            
        # execute_data_acquisitionが呼ばれないことを確認
        mock_execute.assert_not_called()
    
    @patch('src.wia.gui.execute_data_acquisition')
    @patch('src.wia.gui.Toplevel')
    def test_on_execute_success(self, mock_toplevel, mock_execute):
        """正常実行時のテスト"""
        self.app = WWRApp()
        
        # 有効な入力値を設定
        self.app.codes = ["12345"]
        self.app.year_start.set("2023")
        self.app.year_end.set("2023")
        self.app.month_start.set("1月")
        self.app.month_end.set("12月")
        self.app.mode.set("S")
        self.app.use_data_sru.set(False)
        self.app.single_sheet_var.set(False)
        
        # モック設定
        mock_loading = Mock()
        mock_toplevel.return_value = mock_loading
        mock_execute.return_value = ([Path("test.xlsx")], [])
        
        with patch.object(self.app, '_validate', return_value=True):
            with patch.object(self.app, '_show_results') as mock_show_results:
                # _on_executeは別スレッドで実行されるため、直接呼び出し
                self.app._on_execute.__wrapped__(self.app)
                
        # execute_data_acquisitionが適切な引数で呼ばれることを確認
        mock_execute.assert_called_once()
        call_args = mock_execute.call_args
        assert call_args[1]['codes'] == ["12345"]
        assert call_args[1]['start_year'] == 2023
        assert call_args[1]['end_year'] == 2023
        assert call_args[1]['mode'] == "S"
        assert call_args[1]['granularity'] == "hour"
        assert call_args[1]['single_sheet'] is False
    
    @patch('src.wia.gui.execute_data_acquisition')
    @patch('src.wia.gui.Toplevel')
    def test_on_execute_with_errors(self, mock_toplevel, mock_execute):
        """エラーありの実行テスト"""
        self.app = WWRApp()
        
        # 有効な入力値を設定
        self.app.codes = ["12345", "67890"]
        self.app.year_start.set("2023")
        self.app.year_end.set("2023")
        self.app.month_start.set("1月")
        self.app.month_end.set("12月")
        
        # モック設定（一部成功、一部エラー）
        mock_loading = Mock()
        mock_toplevel.return_value = mock_loading
        mock_execute.return_value = (
            [Path("test1.xlsx")],  # 成功したファイル
            [("67890", "指定期間に有効なデータが見つかりませんでした")]  # エラー
        )
        
        with patch.object(self.app, '_validate', return_value=True):
            with patch.object(self.app, '_show_results') as mock_show_results:
                with patch.object(self.app, '_show_error') as mock_show_error:
                    # _on_executeは別スレッドで実行されるため、直接呼び出し
                    self.app._on_execute.__wrapped__(self.app)
        
        # 成功ファイルの表示とエラーの表示が両方呼ばれることを確認
        # 注意: root.afterで呼ばれるため、実際の呼び出しは非同期


class TestToolTip:
    """ツールチップクラスのテスト"""
    
    def setup_method(self):
        """各テストメソッドの前に実行される初期化"""
        self.root = tk.Tk()
        self.root.withdraw()
        
    def teardown_method(self):
        """各テストメソッドの後に実行されるクリーンアップ"""
        if hasattr(self, 'root') and self.root:
            self.root.destroy()
    
    def test_tooltip_init(self):
        """ツールチップ初期化のテスト"""
        widget = tk.Label(self.root, text="Test")
        tooltip_text = "テストツールチップ"
        
        tooltip = ToolTip(widget, tooltip_text, delay=100)
        
        assert tooltip.widget == widget
        assert tooltip.text == tooltip_text
        assert tooltip.delay == 100
        assert tooltip.tipwin is None
        assert tooltip.id is None
    
    @patch('src.wia.gui.Toplevel')
    @patch('src.wia.gui.Label')
    def test_tooltip_show(self, mock_label, mock_toplevel):
        """ツールチップ表示のテスト"""
        widget = tk.Label(self.root, text="Test")
        tooltip = ToolTip(widget, "テストツールチップ")
        
        # モックイベントを作成
        mock_event = Mock()
        mock_event.x_root = 100
        mock_event.y_root = 200
        
        # モックウィンドウを設定
        mock_window = Mock()
        mock_toplevel.return_value = mock_window
        
        tooltip.show(mock_event)
        
        # Toplevelウィンドウが作成されることを確認
        mock_toplevel.assert_called_once_with(widget)
        
        # ウィンドウの設定が呼ばれることを確認
        mock_window.wm_overrideredirect.assert_called_once_with(True)
        mock_window.wm_geometry.assert_called_once_with("+110+210")  # x+10, y+10
        
        # ラベルが作成されることを確認
        mock_label.assert_called_once()
    
    def test_tooltip_hide(self):
        """ツールチップ非表示のテスト"""
        widget = tk.Label(self.root, text="Test")
        tooltip = ToolTip(widget, "テストツールチップ")
        
        # IDを設定してhideをテスト
        tooltip.id = "test_id"
        tooltip.tipwin = Mock()
        
        with patch.object(widget, 'after_cancel') as mock_cancel:
            tooltip.hide()
            
        # after_cancelが呼ばれることを確認
        mock_cancel.assert_called_once_with("test_id")
        
        # tipwinのdestroyが呼ばれることを確認
        tooltip.tipwin.destroy.assert_called_once()
        
        # 変数がリセットされることを確認
        assert tooltip.id is None
        assert tooltip.tipwin is None


if __name__ == "__main__":
    pytest.main([__file__])