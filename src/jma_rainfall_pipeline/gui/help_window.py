"""ヘルプウィンドウモジュール"""
import tkinter as tk
from tkinter import ttk, scrolledtext
from jma_rainfall_pipeline.logger.app_logger import get_logger
from jma_rainfall_pipeline.version import get_version_string

logger = get_logger(__name__)


class HelpWindow:
    """ヘルプウィンドウクラス"""
    
    def __init__(self, parent):
        self.parent = parent
        self.window = None
    
    def show(self):
        """ヘルプウィンドウを表示する"""
        if self.window is not None and self.window.winfo_exists():
            # 既にウィンドウが開いている場合は前面に表示
            self.window.lift()
            self.window.focus_force()
            return
        
        # ヘルプウィンドウを作成
        self.window = tk.Toplevel(self.parent)
        self.window.title("ヘルプ - JMA Rainfall Pipeline")
        self.window.geometry("800x600")
        self.window.minsize(600, 400)
        
        # ウィンドウを閉じる際の処理
        self.window.protocol("WM_DELETE_WINDOW", self._on_close)
        
        # メインフレーム
        main_frame = ttk.Frame(self.window)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # ヘルプ内容を表示するテキストウィジェット
        help_text = scrolledtext.ScrolledText(
            main_frame,
            wrap=tk.WORD,
            font=("メイリオ", 10),
            state=tk.DISABLED
        )
        help_text.pack(fill=tk.BOTH, expand=True)
        
        # ヘルプ内容を設定
        help_content = self._get_help_content()
        help_text.config(state=tk.NORMAL)
        help_text.insert(tk.END, help_content)
        help_text.config(state=tk.DISABLED)
        
        # 閉じるボタン
        button_frame = ttk.Frame(main_frame)
        button_frame.pack(fill=tk.X, pady=(10, 0))
        
        ttk.Button(
            button_frame,
            text="閉じる",
            command=self._on_close
        ).pack(side=tk.RIGHT)
        
        # ウィンドウを中央に配置
        self._center_window()
    
    def _on_close(self):
        """ウィンドウを閉じる"""
        if self.window:
            self.window.destroy()
            self.window = None
    
    def _center_window(self):
        """ウィンドウを画面中央に配置"""
        self.window.update_idletasks()
        width = self.window.winfo_width()
        height = self.window.winfo_height()
        x = (self.window.winfo_screenwidth() // 2) - (width // 2)
        y = (self.window.winfo_screenheight() // 2) - (height // 2)
        self.window.geometry(f"{width}x{height}+{x}+{y}")
    
    def _get_help_content(self):
        """ヘルプ内容を取得"""
        version_info = get_version_string()
        return f"""
JMA Rainfall Pipeline ヘルプ

【概要】
このアプリケーションは気象庁の降水量データを取得・エクスポートするツールです。
取得したデータはCSV形式またはExcel形式で出力でき、データ分析やレポート作成に活用できます。
降水量データのほか、気温、湿度、風速などの気象要素も含まれています。

【基本的な使い方】

1. 期間と間隔の指定
   - 開始日と終了日を入力してください
   - 日付形式: YYYY-MM または YYYY-MM-DD
   - 間隔: daily（日次）、hourly（時間）、10min（10分間隔）から選択
   - クイック選択ボタンで簡単に期間を設定できます

2. 地点の選択
   - 都道府県一覧から都道府県を選択
   - 観測所一覧から観測所を選択（Ctrl+クリックで複数選択可）
   - 表示形式: 観測所名 (JIS都道府県コード-観測所コード)

3. 取得対象の確認
   - 選択した観測所が一覧表示されます
   - 不要な観測所は削除できます（Ctrl+クリックで複数選択可）
   - すべてクリアボタンで一括削除も可能

4. 出力オプション
   - CSVを出力: CSVファイルの出力有無を制御
   - Excelを出力: Excelファイルの出力有無を制御
   - 初期状態: CSV=オフ、Excel=オン

5. データ取得
   - 「データ取得」ボタンをクリックして処理を開始
   - 完了後、出力先フォルダを開くか選択できます

【出力形式について】

■ CSV形式（全データ出力）
- ファイル形式: UTF-8 BOM付きCSV
- 処理内容: 取得した全ての気象データをそのまま出力
- 含まれるデータ項目:
  * 日時情報: datetime, date, time, hour, minute
  * 気圧: pressure_ground, pressure_sea
  * 降水量: precipitation, precipitation_total, precipitation_max_1h, precipitation_max_10m
  * 気温: temperature, temperature_avg, temperature_max, temperature_min, dew_point
  * 湿度: humidity, vapor_pressure
  * 風: wind_speed, wind_direction, wind_speed_max, wind_direction_max
  * 日照・日射: sunshine_hours, sunshine_minutes, solar_radiation
  * 積雪: snow_fall, snow_depth
  * その他: weather, cloud_cover, visibility
- 用途: 全データ分析、統計処理、他のシステムとの連携
- 出力先: outputs/csv ディレクトリ
- ファイル名例: 宗谷地方_稚内_hourly_20250101-20250131.csv

■ Excel形式（降水量データのみ）
- ファイル形式: .xlsx（Excel 2007以降対応）
- 処理内容: CSVの全データから降水量関連項目のみを抽出・整形
- 含まれるデータ項目:
  * 日時: 日時（適切なフォーマットで表示）
  * 降水量: 降水量、合計降水量、1時間最大降水量、10分間最大降水量
- 特徴:
  * 降水量に特化した簡潔なデータ
  * 日時はExcelの日時形式で保存（グラフ作成に最適）
  * 列名は日本語で分かりやすく表示
  * 列幅は自動調整済み
- 用途: 降水量分析、レポート作成、グラフ作成
- 出力先: outputs/excel ディレクトリ
- ファイル名例: 宗谷地方_稚内_hourly_20250101-20250131.xlsx

【データ処理の違い】

■ CSV出力の処理
1. 気象庁から取得した全データをそのまま保存
2. 英語の列名で保存（プログラム処理用）
3. 日時は文字列形式かつプログラムで扱いやすい形で保存
4. 全ての気象要素を含む（20項目以上）

■ Excel出力の処理
1. CSVデータから降水量関連項目のみを抽出
2. 列名を日本語に変換（precipitation→降水量、precipitation_total→合計降水量）
3. 日時をExcelの日時形式に変換
4. 降水量データのみ（2~4項目）で簡潔に表示

【出力先】
- CSVファイル: outputs/csv ディレクトリ
- Excelファイル: outputs/excel ディレクトリ
- 設定は config.yml で変更可能

【観測方式について】
- 気象台ほか (s): 気象台や特別地域気象観測所
- アメダス (a): アメダス観測所

【注意事項】
- データの取得には時間がかかる場合があります
- 大量のデータを取得する場合は、期間を短く区切ることをお勧めします
- 取得可能なデータは過去のデータのみです（リアルタイムデータではありません）
- CSVとExcelの両方を出力する場合、処理時間が長くなる可能性があります

【トラブルシューティング】
- データが取得できない場合: 観測所IDや日付範囲を確認してください
- エラーが発生した場合: ログファイル（logs/app.log）を確認してください
- アプリケーションが応答しない場合: 処理中ですのでしばらくお待ちください
- Excelファイルが開けない場合: Microsoft Excel 2007以降または互換ソフトが必要です

【バージョン情報】
{version_info}
        """
