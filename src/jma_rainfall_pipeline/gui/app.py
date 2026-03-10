# gui/app.py
import tkinter as tk

from water_info_acquirer.app_meta import get_module_title
from water_info_acquirer.navigation import build_navigation_menu
from jma_rainfall_pipeline.gui.browse_tab import BrowseWindow
from jma_rainfall_pipeline.gui.help_window import HelpWindow
from jma_rainfall_pipeline.gui.error_dialog import show_error


class App(tk.Toplevel):
    def __init__(self, parent: tk.Misc, on_open_other=None, on_close=None, on_return_home=None):
        """JMA GUI本体（Toplevel）。"""
        super().__init__(parent)
        self.on_open_other = on_open_other
        self.on_close = on_close
        self.on_return_home = on_return_home

        try:
            self.title(get_module_title("jma", lang="jp"))
            # ウィンドウサイズ設定
            self.geometry("800x800")
            self.minsize(800, 600)

            # ヘルプウィンドウのインスタンス
            self.help_window = HelpWindow(self)

            self.config(
                menu=build_navigation_menu(
                    self,
                    current_app_key="jma",
                    on_open_other=self._open_other,
                    on_return_home=self._return_home,
                )
            )

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
            raise

    def _open_other(self, app_key: str):
        """別アプリを開くリクエストを上位に通知。"""
        if self.on_open_other:
            self.destroy()
            self.on_open_other(app_key)

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

    def _return_home(self):
        try:
            self.destroy()
        finally:
            if self.on_return_home:
                self.on_return_home()

    def handle_error(self, exc_type, exc_value, exc_traceback):
        """捕捉されない例外をここでまとめて処理。"""
        error_msg = str(exc_value) if exc_value else "不明なエラー"
        show_error(self, "予期せぬエラー",
                  f"予期せぬエラーが発生しました: {error_msg}",
                  exc_value)


def show_jma(parent: tk.Misc, on_open_other=None, on_close=None, on_return_home=None) -> App:
    """ランチャー/親Tkから呼び出すファクトリ。"""
    return App(parent=parent, on_open_other=on_open_other, on_close=on_close, on_return_home=on_return_home)


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
