# 気象庁側の設定（config.yml）

気象庁 雨量データ取得の出力先・ログなどは `src/jma_rainfall_pipeline/config.yml` で調整できます。

## 主な項目

- `output.csv_dir`: CSV出力先（相対パスの場合はプロジェクトルート基準）
- `output.excel_dir`: Excel出力先（相対パスの場合はプロジェクトルート基準）
- `logging.file`: ログファイル出力先
- `enable_station_cache`: 観測所一覧キャッシュの有効/無効

!!! note "画面で出力フォルダを選択した場合"
    GUIで「出力フォルダを選択」した場合は、設定ファイルより優先して、そのフォルダ配下に `csv/`, `excel/`, `logs/` を作成して出力します。

