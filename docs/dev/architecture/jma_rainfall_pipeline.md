# JMA rainfall pipeline architecture

`src/jma_rainfall_pipeline/` は、気象庁の雨量データ取得と出力を担当する。

## 入口

- `src/jma_rainfall_pipeline/__main__.py`
  - `python -m jma_rainfall_pipeline` の起動入口。
- `src/jma_rainfall_pipeline/main.py`
  - GUI 起動の入口。
- `src/jma_rainfall_pipeline/cli.py`
  - CLI 引数の解釈とコマンド分岐。
- `src/jma_rainfall_pipeline/launcher_entry.py`
  - アプリ選択ランチャーから開く入口。

## 責務

- JMA の観測地点検索と取得条件設定
- 短時間、10分、日別などの雨量取得
- CSV / Excel / Parquet / NDJSON 出力
- GUI と CLI の両方の実行経路提供

## 主な層

- `gui/`
  - ブラウズ取得画面、自由入力画面、ヘルプ、エラーダイアログを置く。
- `controller/`
  - GUI/CLI から受けた要求を取得・出力へ橋渡しする。
- `fetcher/`
  - JMA のコード探索、自由入力パース、観測所選択補助を置く。
- `parser/`
  - 表形式 HTML の解析を置く。
- `exporter/`
  - CSV / Excel / Parquet / NDJSON の書き出しを置く。
- `api/`
  - HTTP 経由の公開 API 層を置く。
- `utils/`
  - パス、日付、設定、キャッシュ、HTTP の共通補助を置く。
- `domain/`
  - データモデルと、天気データのルールを置く。
- `infrastructure/`
  - データリポジトリなどの外部依存を置く。
- `logger/`
  - 実行ログの出力を置く。

## データの流れ

1. GUI または CLI が取得条件を受け取る。
2. `controller/weather_data_controller.py` が取得単位を決める。
3. `fetcher/` と `parser/` が JMA の表を取り込む。
4. `domain/models.py` と `domain/services/weather_services.py` が意味づけを行う。
5. `exporter/` が各形式へ保存する。

## 補足

- 取得後の時刻は `period_end_at` を正とする。
- 出力はパッケージ内の exporter を通して行う。
- GUI と CLI は同じ controller を共有する。
