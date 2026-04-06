# 時間意味と境界取得の設計整理

## 背景

`water_info` と `JMA` の時間データについて、旧実装との差分や境界運用を明示し、現行契約を固定する。

特に以下が問題になる。

1. 旧実装では `water_info` の `S` / `R` を 1 時間戻した区間値のように扱っていたため、瞬間値契約へ移行した。
2. `U` や `JMA` の雨量は区間値として扱う前提があるが、境界時刻の構築のために前後のデータ取得範囲を明示できていない。
3. `request_window` と `fetch_window` と `publish_window` が分離されておらず、境界を埋めるための余分取得と最終出力範囲の切り戻しが設計として固定されていない。

この資料では、時間意味と取得境界のルールを統一し、後続実装の判断を不要にすることを目的とする。

## 用語定義

- `request_window`
  - ユーザーが欲しい論理期間
  - 例: `2025-01-01 00:00:00` から `2025-01-04 00:00:00` の排他的上限
- `fetch_window`
  - 境界レコードを構築するために、実際に取得する期間
  - `request_window` より広くなることがある
- `publish_window`
  - 正規化後に CSV / Excel / Parquet / グラフへ最終反映する期間
  - 原則として `request_window` と一致させる
- `observed_at`
  - 時系列上でレコードを代表させる時刻
- `period_start_at`
  - 区間値の開始時刻
- `period_end_at`
  - 区間値の終了時刻
- `value_semantics`
  - 値の意味
  - `instantaneous` または `interval`

## 正式ルール

### 1. 指標ごとの時間意味

#### water_info

- `S` 水位
  - `value_semantics = instantaneous`
- `R` 流量
  - `value_semantics = instantaneous`
- `U` 雨量
  - `value_semantics = interval`

#### JMA

- `hourly` / `10min` / `daily` の降水量
  - `value_semantics = interval`

### 2. unified schema の意味

#### 瞬間値

- `observed_at`
  - 実観測時刻そのもの
- `period_start_at`
  - `NULL`
- `period_end_at`
  - `NULL`

適用対象:

- `water_info:S`
- `water_info:R`

#### 区間値

- `observed_at`
  - 区間終端時刻
- `period_start_at`
  - 区間開始時刻
- `period_end_at`
  - 区間終了時刻

適用対象:

- `water_info:U`
- `JMA rainfall`

### 3. publish_window での切り方

- 瞬間値
  - `publish_window` 判定は `observed_at` に対して行う
  - 判定式は `request_start <= observed_at <= request_end_exclusive`
- 区間値
  - `publish_window` 判定は `period_end_at` に対して行う
  - 判定式は `request_start <= period_end_at <= request_end_exclusive`

理由:

1. 瞬間値は観測時刻そのものが主キーであるため
2. 区間値は終端時刻で時系列整列する方針とするため

## 境界取得ルール

## 基本方針

出力側で不足が起きないよう、常に以下の順で処理する。

1. `request_window` を受け取る
2. `value_semantics` と `interval` に応じて `fetch_window` を拡張する
3. 取得元データを正規化する
4. unified records を作る
5. `publish_window` で切り戻す
6. CSV / Excel / Parquet / グラフへ反映する

## fetch_window 拡張ルール

### water_info:S / R

- 瞬間値
- 取得元時刻に `24:00` 相当表現が混ざりうるため、`request_window` の先頭を正しく作るには 1 ステップ前が必要
- `hourly` の場合:
  - `fetch_window.start = request_window.start - 1 hour`
  - `fetch_window.end = request_window.end`
- `daily` の場合:
  - `fetch_window.start = request_window.start - 1 day`
  - `fetch_window.end = request_window.end`

### water_info:U

- 区間値
- request の先頭区間を作るには、終端側レコードが必要
- `hourly` の場合:
  - `fetch_window.start = request_window.start`
  - `fetch_window.end = request_window.end`
  - ただし取得元の表記都合で先頭 `00:00` を前日 `24:00` として持つ場合は 1 ステップ前を追加取得する
- `daily` の場合:
  - `fetch_window.start = request_window.start`
  - `fetch_window.end = request_window.end`
  - 同様に取得元の終端表記都合がある場合は 1 日前を追加取得する

### JMA rainfall

- すべて区間値
- 利用者向けの終了指定は「当日末尾」ではなく `request_end_exclusive` として扱う
- 例: `2026-03-03` 指定は内部的に `2026-03-04 00:00:00` を意味する
- `hourly`
  - `fetch_window.start = request_window.start - 1 hour`
  - `fetch_window.end = request_window.end`
- `10min`
  - `fetch_window.start = request_window.start - 10 min`
  - `fetch_window.end = request_window.end`
- `daily`
  - `fetch_window.start = request_window.start`
  - `fetch_window.end = request_window.end`

補足:

- 旧来の `24時` / `23:59:59.999999` 吸収は `fetch_window` と正規化の責務として扱う
- 出力時に補正しない

## 現状実装からの修正点

### water_info

1. `S` / `R` は区間化をやめる
2. `observed_at` を取得元時刻として保持する
3. `period_start_at` / `period_end_at` は `NULL` にする
4. Excel の時間列は `period_end_at` ではなく `observed_at` を主に使う
5. request 範囲先頭の瞬間値を作るため、1 ステップ前の取得を導入する

### water_info:U

1. `interval` ごとの区間値として統一する
2. 区間は `period_start_at` / `period_end_at` で保持する
3. `observed_at = period_end_at` とする
4. 先頭区間が欠けないように、必要な場合は 1 ステップ前を追加取得する

### JMA

1. `hourly` / `10min` / `daily` を区間値として扱う
2. `observed_at = period_end_at` を維持する
3. `24時` や `23:59:59.999999` の吸収は正規化段階に限定する
4. `CSV` の `hour=24` のような表示残りは排除し、最終的に実時刻へ寄せる
5. `10min` の `period_start_at=...59.999999` のような疑似境界を排除する
6. 先頭の `0:00` 終端区間も保持する
7. `request_end` の実装で `23:59:59.999999` を生成しない

## 影響範囲

### 直接影響

- `src/water_info/service/flow_fetch.py`
- `src/water_info/infra/dataframe_utils.py`
- `src/water_info/entry.py`
- `src/water_info/service/flow_write.py`
- `src/water_info/cli.py`
- `src/jma_rainfall_pipeline/controller/weather_data_controller.py`
- `src/jma_rainfall_pipeline/exporter/csv_exporter.py`
- `src/jma_rainfall_pipeline/exporter/parquet_exporter.py`
- `src/hydrology_graphs/io/parquet_store.py`

### 間接影響

- `hydrology_graphs` の時系列解釈
- `river_meta` の共通フォーマット化ロジック
- 既存 Parquet の互換方針
- CSV / Excel のヘッダー説明とドキュメント

## 実装方針

### Phase 1: スキーマ定義の修正

1. `value_semantics` を内部設計に追加する
2. `observed_at` / `period_start_at` / `period_end_at` の意味を正式固定する
3. `S/R/U/JMA` の対応表をコードコメントではなく設計として残す

### Phase 2: 取得窓の分離

1. `request_window` / `fetch_window` / `publish_window` を明示的に導入する
2. 取得元ごとに `fetch_window` 拡張関数を実装する
3. 実データ取得前に拡張済み window を計算する

### Phase 3: 正規化の修正

1. `water_info:S/R` を瞬間値として正規化する
2. `water_info:U` を区間値として正規化する
3. `JMA hourly/10min` の境界計算を実時間ベースに置換する

### Phase 4: 出力の修正

1. CSV / Excel / Parquet で同じ時間意味を使う
2. Excel 時刻列を `value_semantics` に応じて切り替える
3. CLI 出力要約も `observed_at` 中心へ揃える

### Phase 5: 後段利用の修正

1. `hydrology_graphs` が瞬間値と区間値を区別できるようにする
2. 必要なら描画側で `value_semantics` を見て補助処理を分ける

## テスト項目

### water_info:S / R

1. 先頭 `00:00` を request したとき、前日追加取得により `observed_at=00:00` が構築できる
2. `period_start_at` / `period_end_at` が `NULL` で保存される
3. Excel / CSV / Parquet が同じ時刻意味になる

### water_info:U

1. 先頭区間が欠けない
2. `observed_at = period_end_at`
3. `period_start_at` / `period_end_at` が interval に一致する

### JMA

1. `hourly` で `hour=24` が残らない
2. `10min` で `59.999999` 境界が残らない
3. `publish_window` で切り戻した結果が request 範囲に一致する

### 共通

1. 月跨ぎ
2. 年跨ぎ
3. request 先頭・末尾の境界
4. 欠測を含むケース
5. 旧 Parquet との差分確認

## 判断済み事項

1. `water_info:S/R` は瞬間値とする
2. `water_info:U` は区間値とする
3. `JMA rainfall` は区間値とする
4. 出力切り戻しは `publish_window` で行う
5. 境界を埋めるための余分取得は `fetch_window` の責務とする

## 保留事項

1. 既存 Parquet を再生成するか、互換差分として併存させるか
2. `value_semantics` を Parquet の列として明示追加するか、metric/source/interval から導出するか
3. `water_info daily:S/R` の取得元時刻が厳密に瞬間値とみなせるかの最終確認
