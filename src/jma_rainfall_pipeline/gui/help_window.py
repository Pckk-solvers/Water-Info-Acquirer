"""ヘルプウィンドウ"""
import tkinter as tk
from tkinter import ttk, scrolledtext
from jma_rainfall_pipeline.logger.app_logger import get_logger
from src.app_names import get_module_title

logger = get_logger(__name__)


class HelpWindow:
    """ヘルプウィンドウクラス"""

    def __init__(self, parent):
        self.parent = parent
        self.window = None

    def show(self):
        """ヘルプウィンドウを表示する"""
        if self.window is not None and self.window.winfo_exists():
            self.window.lift()
            self.window.focus_force()
            return

        self.window = tk.Toplevel(self.parent)
        self.window.title(f"ヘルプ - {get_module_title('jma', lang='jp')}")
        self.window.geometry("800x600")
        self.window.minsize(600, 400)
        self.window.protocol("WM_DELETE_WINDOW", self._on_close)

        main_frame = ttk.Frame(self.window)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        help_text = scrolledtext.ScrolledText(
            main_frame,
            wrap=tk.WORD,
            font=("メイリオ", 10),
            state=tk.DISABLED
        )
        help_text.pack(fill=tk.BOTH, expand=True)

        help_content = self._get_help_content()
        help_text.config(state=tk.NORMAL)
        help_text.insert(tk.END, help_content)
        help_text.config(state=tk.DISABLED)

        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=(10, 0))

        ttk.Button(
            button_frame,
            text="閉じる",
            command=self._on_close
        ).pack(side=tk.RIGHT)

        self._center_window()

    def _on_close(self):
        """ウィンドウを閉じる"""
        if self.window:
            self.window.destroy()
            self.window = None

    def _center_window(self):
        """ウィンドウを中央に配置"""
        self.window.update_idletasks()
        width = self.window.winfo_width()
        height = self.window.winfo_height()
        x = (self.window.winfo_screenwidth() // 2) - (width // 2)
        y = (self.window.winfo_screenheight() // 2) - (height // 2)
        self.window.geometry(f"{width}x{height}+{x}+{y}")

    def _get_help_content(self):
        """ヘルプテキスト"""
        title = get_module_title("jma", lang="jp")
        return f"""
{title} ヘルプ

【概要】
このアプリケーションは気象庁の雨量データを取得・エクスポートするツールです。
取得したデータはCSVまたはExcelとして出力し、レポート作成に活用できます。

【主な使い方】
1. 期間と粒度の設定: 開始日と終了日を入力し、粒度（例: hourly）を選択してください。
2. 観測所の選択: 都道府県や観測所一覧から対象を選択してください。
3. 出力オプション: CSV/Excel の出力有無を選択してください。
4. データ取得: 「データ取得」ボタンを押すと取得と保存を開始します。

【出力先】
- CSV: outputs/csv
- Excel: outputs/excel
- 設定は config.yml で変更可能

【トラブルシューティング】
- データが取得できない場合は観測所ID・期間・ネットワークを確認してください。
- エラー時は logs/app.log を確認してください。
        """
