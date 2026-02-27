# 気象庁側の設定

現在、気象庁 雨量データ取得は `config.yml` などの設定ファイルを使用しません。

## 既定値

- CSV出力先: `outputs/jma/csv`
- Excel出力先: `outputs/jma/excel`
- ログ出力先: `outputs/jma/jma_app.log`
- 観測所一覧キャッシュ: 既定で有効（`river_rainfall.py` 経由実行時は無効）

!!! note "画面で出力フォルダを選択した場合"
    GUIで「出力フォルダを選択」した場合は、選択フォルダ配下に `csv/`, `excel/`, `logs/` を作成して出力します。
