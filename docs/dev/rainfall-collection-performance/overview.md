# 雨量整理取得 高速化 Overview（JMA / WaterInfo）

## 1. 目的
- 「整理取得（GUIのデータ取得）」が長時間化する原因を、現行実装ベースで特定する。
- JMA / WaterInfo の両系統で、効果の大きい改善ポイントを優先順位付きで整理する。
- この後の要件定義で実装可能な粒度まで落とす。

## 2. 調査スコープ（実装確認日: 2026-03-05）
- `src/river_meta/services/rainfall.py`
- `src/river_meta/rainfall/jma_adapter.py`
- `src/jma_rainfall_pipeline/fetcher/fetcher.py`
- `src/jma_rainfall_pipeline/utils/http_client.py`
- `src/river_meta/rainfall/waterinfo_adapter.py`
- `src/water_info/service/flow_fetch.py`
- `src/water_info/infra/http_client.py`
- `src/river_meta/rainfall/parquet_store.py`

## 3. 現行フローの性能特性
### 3.1 JMA（hourly）
- `run_rainfall_analyze()` が観測所×年でループ。
- 1年の取得で `_fetch_jma_year_monthly()` が 1〜12月を順次処理。
- 月取得は `_collect_jma_with_resolved()` -> `fetch_jma_rainfall()` を呼ぶ。
- `Fetcher.schedule_fetch()` は hourly の場合「日単位」でHTTPを投げる。
  - つまり概算で `365 リクエスト / 年 / 観測所`（うるう年は 366）。

### 3.2 WaterInfo（hourly）
- `run_rainfall_analyze()` が観測所×年で `_fetch_waterinfo_year()` を呼ぶ。
- `fetch_hourly_dataframe_for_code()` は対象年の月数ぶん URL を順次取得。
  - 概算で `12 リクエスト / 年 / 観測所`。
- さらに年ごとに観測所名取得の初回URL呼び出しが 1 回あり、合計概算 `13 リクエスト / 年 / 観測所`。

### 3.3 共通HTTP遅延
- JMA / WaterInfo とも `throttled_get` に遅延制御あり。
- 遅延はリクエスト回数に応じて増加し、上限 `2.0 秒` で張り付きやすい設定。
- 長期間処理では待機時間が支配的になる。

## 4. 50年×1観測所の概算（キャッシュ未ヒット前提）
- JMA:
  - リクエスト数: `約 18,250`（= 365 × 50）
  - 遅延のみ概算: `約 10.1 時間`（= 18,250 × 2秒）
- WaterInfo:
  - リクエスト数: `約 650`（= 13 × 50）
  - 遅延のみ概算: `約 21.7 分`（= 650 × 2秒）

## 5. ボトルネック（原因）
1. JMAは日単位HTTPでリクエスト数が非常に多い。
2. HTTP遅延設定が固定的で、長期処理時に待機時間が支配する。
3. GUIの「データ取得」でも `run_rainfall_analyze()` を使うため、Excel/グラフ非出力でも時系列・年最大の集計処理を実行している。
4. JMAは月Parquet保存後に再読込・結合しており、取得専用実行時には不要なI/Oが発生する。
5. WaterInfoは年ごとに観測所名取得を行うため、同一観測所で重複HTTPがある。

## 6. 改善案（優先度順）
### P0（最優先）
1. 取得専用高速モード（collect-fast）を導入し、`export_excel=False` かつ `export_chart=False` のときは集計処理をスキップする。
2. 取得専用では「Parquet保存まで」で終了し、月Parquet再読込・年結合を行わない。

### P1（高優先）
3. 観測所単位の並列取得（上限付き）を導入する。
4. 並列時のHTTP制御を「グローバル遅延カウンタ依存」から「明示的な同時実行数/待機制御」へ見直す。

### P2（中優先）
5. WaterInfo観測所名をメモリキャッシュし、年跨ぎ取得で再取得しない。
6. WaterInfoにも年存在チェックをオプション導入し、不要年の取得を事前に削減する。

## 7. 期待効果（現実的な目安）
- P0だけでも、CPU/I/Oの無駄削減で体感速度は大きく改善（特に大量年数時）。
- P1導入後は、ネットワーク待ちが主因のため、運用上許容できる範囲でほぼ線形に短縮可能。
- JMAの本質的な重さ（1日1HTTP）自体は残るため、最終的には「安全な並列化」と「不要処理削減」の組み合わせが主戦略。

## 8. 注意点
- 取得先への負荷制御は必須。速度優先で過剰並列にすると429/遮断リスクがある。
- 停止時は新規Parquetを保持する方針で、欠け月再取得と整合させる。
- Parquet互換性は維持する前提で段階導入する。
