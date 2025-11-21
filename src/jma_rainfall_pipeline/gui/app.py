# gui/app.py
import tkinter as tk
from tkinter import ttk
import traceback
import sys

from jma_rainfall_pipeline.gui.browse_tab import BrowseWindow
from jma_rainfall_pipeline.gui.help_window import HelpWindow
from jma_rainfall_pipeline.version import get_full_title
# from jma_rainfall_pipeline.gui.freeform_tab import FreeformTab # 実装を予定しておく
from jma_rainfall_pipeline.gui.error_dialog import show_error

class App(tk.Tk):
    def __init__(self):
        try:
            super().__init__()
            self.title(get_full_title())
            
            # ウィンドウサイズを設定
            self.geometry("800x800")
            self.minsize(800, 600)
            
            # ヘルプウィンドウのインスタンスを作成
            self.help_window = HelpWindow(self)
            
            # メインコンテンツの配置
            self.browse_window = BrowseWindow(self)
            self.browse_window.pack(expand=True, fill="both", padx=10, pady=5)
            
            # ヘルプボタンを追加
            self._create_help_button()
            
            # エラーハンドラの設定
            self.report_callback_exception = self.handle_error
            
        except Exception as e:
            # 初期化中のエラーを処理
            self.destroy()  # メインウィンドウを破棄
            show_error(None, "アプリケーションエラー", 
                     "アプリケーションの初期化中にエラーが発生しました。", e)
            sys.exit(1)
    
    def _create_help_button(self):
        """ヘルプボタンを作成する"""
        # ヘルプボタンを右上に配置
        help_button = ttk.Button(
            self,
            text="ヘルプ",
            command=self._show_help
        )
        help_button.place(relx=0.95, rely=0.02, anchor=tk.NE)
    
    def _show_help(self):
        """ヘルプウィンドウを表示する"""
        try:
            self.help_window.show()
        except Exception as e:
            show_error(self, "ヘルプエラー", 
                      "ヘルプウィンドウの表示中にエラーが発生しました。", e)
    
    def handle_error(self, exc_type, exc_value, exc_traceback):
        """ハンドルされていない例外を処理する"""
        error_msg = str(exc_value) if exc_value else "不明なエラー"
        show_error(self, "予期せぬエラー", 
                  f"予期せぬエラーが発生しました: {error_msg}", 
                  exc_value)

def main():
    """アプリケーションのエントリーポイント"""
    try:
        app = App()
        app.mainloop()
    except Exception as e:
        # メインループでのエラーを処理
        show_error(None, "致命的なエラー", 
                  "アプリケーションで致命的なエラーが発生しました。", e)
        sys.exit(1)

if __name__ == "__main__":
    main()
