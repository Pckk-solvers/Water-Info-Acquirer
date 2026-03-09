# 雨量 Parquet 期間抽出 タスク

## Phase 1: service 基盤

1. 入出力モデル追加
- `RainfallParquetPeriodExportInput`
- `RainfallParquetPeriodExportResult`

2. service 実装
- `run_rainfall_parquet_period_export()` を追加
- JMA / WaterInfo の Parquet 読込分岐を実装
- `build_hourly_timeseries_dataframe()` を使った 24 時間整形を実装

3. CSV 書き出し
- `date,hour,rainfall` の DataFrame を CSV 保存
- ファイル名 `{source}_{station_code}_{station_name}_{date}.csv` を実装

## Phase 2: GUI 新規タブ

1. 新規タブ追加
- `期間CSV出力` タブを notebook に追加

2. UX 実装
- Parquet ディレクトリ入力
- スキャンボタン
- 観測所一覧
- 対象日付入力
- 出力ディレクトリ入力
- 実行ボタン
- ログ表示

3. 入力構築
- 一覧選択から `source/station_key/station_name` を組み立てる
- 日付入力を `RainfallParquetPeriodExportInput` に変換する

## Phase 3: テスト

1. service テスト
- JMA 抽出
- WaterInfo 抽出
- 欠測時間
- ファイル名
- データなし

2. GUI テスト
- スキャン結果選択
- 入力組み立て
- 実行イベント

3. 回帰
- `uv run pytest tests/river_meta -q`

## Phase 4: 仕上げ

1. ログ文言調整
- 実行開始
- スキャン結果
- 出力成功
- エラー

2. ドキュメント更新
- `docs/dev/index.md`
- 必要なら詳細設計の補足

## リスク

1. JMA と WaterInfo のコード表示差
- GUI 表示コードと内部 `station_key` を分ける

2. 欠測行の扱いぶれ
- 24 行固定をテストで保証する

3. UI の複雑化
- 既存 selector を流用せず、Parquet スキャン起点の最短導線に限定する

