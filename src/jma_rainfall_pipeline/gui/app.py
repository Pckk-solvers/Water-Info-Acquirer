# gui/app.py
import sys
import tkinter as tk
from tkinter import ttk
import traceback

from src.app_names import get_module_title
from jma_rainfall_pipeline.gui.browse_tab import BrowseWindow
from jma_rainfall_pipeline.gui.help_window import HelpWindow
from jma_rainfall_pipeline.gui.error_dialog import show_error


class App(tk.Toplevel):
    def __init__(self, parent: tk.Misc, on_open_other=None, on_close=None):
        """JMA GUI本体（Toplevel）。"""
        super().__init__(parent)
        self.on_open_other = on_open_other
        self.on_close = on_close

        try:
            self.title(get_module_title("jma", lang="jp"))
            # ウィンドウサイズ設定
            self.geometry("800x800")
            self.minsize(800, 600)

            # ヘルプウィンドウのインスタンス
            self.help_window = HelpWindow(self)

            # メニューバー（ヘルプと水文遷移）
            menubar = tk.Menu(self)
            nav_menu = tk.Menu(menubar, tearoff=0)
            nav_menu.add_command(label=get_module_title("water_info", lang="jp"), command=self._open_water)
            nav_menu.add_command(label="ヘルプ", command=self._show_help)
            menubar.add_cascade(label="メニュー", menu=nav_menu)
            self.config(menu=menubar)

            # メインコンテナの配置
            self.browse_window = BrowseWindow(self)
            self.browse_window.pack(expand=True, fill="both", padx=10, pady=5)

            # エラーハンドラの設定
            self.report_callback_exception = self.handle_error

            # 閉じる挙動
            self.protocol("WM_DELETE_WINDOW", self._handle_close)
            self.deiconify()
            self.lift()
            self.focus_force()
        except Exception as e:
            # 初期化中のエラーはダイアログ表示して終了
            self.destroy()
            show_error(None, "アプリケーションエラー",
                       "アプリケーションの初期化中にエラーが発生しました。", e)
            if self.on_close:
                self.on_close()
            sys.exit(1)

    def _open_water(self):
        """水文アプリを開くリクエストを上位に通知。"""
        if self.on_open_other:
            self.destroy()
            self.on_open_other("water")

    def _show_help(self):
        """ヘルプウィンドウを表示する。"""
        try:
            self.help_window.show()
        except Exception as e:
            show_error(self, "ヘルプエラー",
                      "ヘルプウィンドウの表示中にエラーが発生しました。", e)

    def _handle_close(self):
        """閉じるときの処理。"""
        try:
            self.destroy()
        finally:
            if self.on_close:
                self.on_close()

    def handle_error(self, exc_type, exc_value, exc_traceback):
        """捕捉されない例外をここでまとめて処理。"""
        error_msg = str(exc_value) if exc_value else "不明なエラー"
        show_error(self, "予期せぬエラー",
                  f"予期せぬエラーが発生しました: {error_msg}",
                  exc_value)


def show_jma(parent: tk.Misc, on_open_other=None, on_close=None) -> App:
    """ランチャー/親Tkから呼び出すファクトリ。"""
    return App(parent=parent, on_open_other=on_open_other, on_close=on_close)


def main() -> None:
    """スタンドアロン実行時の入口。"""
    root = tk.Tk()
    root.withdraw()  # 空のrootを隠す

    def _on_close():
        root.destroy()

    show_jma(parent=root, on_open_other=None, on_close=_on_close)
    root.mainloop()


if __name__ == "__main__":
    main()
