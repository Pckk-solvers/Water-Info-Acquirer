# 雨量 Parquet 期間抽出 詳細設計

## 1. 目的

- Parquet に保存済みの雨量データから、指定観測所・指定日付の 24 時間データを CSV 出力する。
- 外部サイトへの再取得は行わず、既存 Parquet を再利用する。
- GUI は既存タブの見た目踏襲ではなく、本機能に最適化した UX を優先する。

## 2. 変更対象

- `src/river_meta/services/`
  - 新規 service を追加
- `src/river_meta/rainfall/gui.py`
  - 新規タブを追加
- `src/river_meta/rainfall/parquet_store.py`
  - 既存読込関数を利用（必要なら補助関数追加）
- `tests/river_meta/`
  - service / GUI 入力構築 / CSV 出力のテスト追加

## 3. ユースケース分離

既存の `run_rainfall_generate()` は「完全年の Excel / グラフ生成」であり、本機能とは責務が異なる。

そのため、別 service を新設する。

- 新規 API
  - `run_rainfall_parquet_period_export(...)`

この API は以下のみを責務とする。

- Parquet ディレクトリの検証
- Parquet から対象観測所・対象日付の抽出
- 24 時間軸への整形
- CSV 出力

## 4. 入出力モデル

### 4.1 入力モデル

新規 dataclass を追加する。

- `RainfallParquetPeriodExportInput`

想定フィールド:

- `parquet_dir: str`
- `output_dir: str`
- `source: str`
- `station_key: str`
- `station_name: str = ""`
- `target_date: date | str`

補足:

- `target_date` は GUI では `YYYY-MM-DD` 文字列入力でもよいが、service 入口で `date` に正規化する。
- `station_name` はファイル名とログ用。未指定時は Parquet から推定する。

### 4.2 出力モデル

新規 dataclass を追加する。

- `RainfallParquetPeriodExportResult`

想定フィールド:

- `csv_path: str | None`
- `row_count: int`
- `source: str`
- `station_key: str`
- `station_name: str`
- `target_date: str`
- `errors: list[str]`

## 5. CSV 仕様

### 5.1 列

第1段階は以下 3 列固定。

- `date`
- `hour`
- `rainfall`

### 5.2 値仕様

- `date`
  - `YYYY-MM-DD`
- `hour`
  - 1-24 表記
  - 既存 Excel と同じ
- `rainfall`
  - `1時間雨量(mm)` の値
  - 欠測時は空欄

### 5.3 行数

- 指定日について 24 行固定
- 実データがない時間も出力する

## 6. Parquet 読込設計

### 6.1 source 差の吸収

- JMA
  - `load_and_concat_monthly_parquets(output_dir, "jma", station_key, year)`
- WaterInfo
  - `load_records_parquet(build_parquet_path(output_dir, "water_info", station_key, year))`

対象年は `target_date.year` で決定する。

### 6.2 不完全年の扱い

本機能では `complete/incomplete` を出力可否条件にしない。

- JMA は対象年の該当月 Parquet があれば読める範囲で出力対象
- WaterInfo は対象年 Parquet があれば出力対象

## 7. 時間軸整形設計

### 7.1 基本方針

既存 Excel と同じ正規化ルールを利用するため、service 内では以下を使う。

- `build_hourly_timeseries_dataframe()`

流れ:

1. Parquet から `source_df` を取得
2. `build_hourly_timeseries_dataframe(source_df)` を実行
3. `観測時刻` で対象日を抽出
4. 0:00-23:00 の 24 時間に reindex
5. `date,hour,rainfall` に整形

### 7.2 日付抽出

対象日 `D` に対して、内部時間軸は以下。

- `D 00:00:00` 〜 `D 23:00:00`

`hour` 列は以下で計算する。

- `観測時刻.hour + 1`

### 7.3 欠測

reindex 時に値が無い時間は `rainfall = NaN` になる。

CSV 出力時は空欄にする。

## 8. ファイル名設計

ファイル名は以下。

- `{source}_{station_code}_{station_name}_{date}.csv`

ここでの `station_code` は以下。

- JMA: GUI 表示コード（`block_no`）
- WaterInfo: `station_key`

内部 `station_key` と表示コードを混同しないため、service には

- `station_key`（内部用）
- `display_station_code`（ファイル名用）

を分けて渡す設計でもよい。

第1段階では、GUI 側で表示コードを解決して入力へ渡す。

## 9. GUI 設計

## 9.1 タブ構成

新規タブ名（仮）:

- `期間CSV出力`

### 9.2 UX 方針

既存取得タブの selector 群は流用しない。

理由:

- 本機能は「既にある Parquet から選ぶ」ユースケース
- 外部マスタ検索 UI を持ち込むと、Parquet が存在しない観測所を選べてしまう

そのため、UI は以下の順序で使える構成にする。

1. Parquet ディレクトリを指定
2. スキャン
3. 観測所一覧から選択
4. 対象日付を入力
5. 出力ディレクトリを指定
6. 実行

### 9.3 推奨レイアウト

1画面内で完結する 2 カラム構成を採用する。

- 左
  - Parquet ディレクトリ
  - スキャンボタン
  - 観測所一覧
- 右
  - 選択中観測所サマリ
  - 対象日付入力
  - 出力ディレクトリ
  - 実行ボタン
  - ログ

### 9.4 観測所一覧

一覧は観測所単位で見せる。

列候補:

- データ元
- 観測所コード
- 観測所名
- 利用可能年範囲

補足:

- 年は独立入力にしないため、一覧の付加情報として表示するだけでよい
- 1行選択前提

### 9.5 入力ウィジェット

- 対象日付
  - `YYYY-MM-DD` の Entry
  - 必要なら簡易バリデーション
- 出力ディレクトリ
  - Entry + Browse

### 9.6 ログ

表示内容:

- スキャン結果件数
- 選択観測所
- 出力先
- CSV 出力完了パス
- エラー詳細

## 10. service 処理フロー

1. 入力検証
   - `parquet_dir` 存在確認
   - `target_date` 形式確認
   - `station_key` 空欄確認
   - `output_dir` 解決
2. source ごとの Parquet 読込
3. `build_hourly_timeseries_dataframe()` 実行
4. 対象日 24 時間へ再整形
5. CSV DataFrame 構築
6. ファイル名生成
7. CSV 書き出し
8. result 返却

## 11. エラー設計

### 11.1 想定エラー

- `parquet/` が見つからない
- 対象観測所 Parquet が存在しない
- 指定日付が不正
- 対象日に1件もデータがない
- CSV 保存失敗

### 11.2 表示方針

- service は `errors` に格納して返す
- GUI はログへ出し、必要なら messagebox を出す

## 12. テスト設計

### 12.1 service

1. JMA 単日抽出
- 対象日 24 行が出ること

2. WaterInfo 単日抽出
- 対象日 24 行が出ること

3. 欠測時間
- 欠測時間も 24 行に含まれ、`rainfall` が空欄になること

4. ファイル名
- `{source}_{station_code}_{station_name}_{date}.csv` になること

5. 対象日データなし
- `csv_path=None` と適切な `errors` を返すこと

### 12.2 GUI

1. スキャン結果から選択できること
2. 日付・出力先が入力されること
3. `RainfallParquetPeriodExportInput` が正しく組み立つこと

## 13. 実装時の注意

1. `generate` の complete 判定ロジックを流用しないこと
2. 表示コードと内部 `station_key` を混同しないこと
3. `build_hourly_timeseries_dataframe()` の正規化を再実装しないこと
4. GUI は既存ウィジェットの見た目踏襲ではなく、操作順が自然かを優先すること

