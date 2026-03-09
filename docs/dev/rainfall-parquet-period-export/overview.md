# 雨量 Parquet 期間抽出 Overview

## 背景

既存の雨量 GUI には以下の2系統がある。

- データ取得: `src/river_meta/services/rainfall.py`
- 整理・出力（Excel / グラフ）: `src/river_meta/services/rainfall.py`

次の新機能として、Parquet から指定期間のデータだけを抽出し、CSV として出力する機能を追加したい。

- 入力: Parquet ディレクトリ、Parquet リストから選んだ観測所、日付
- 出力: CSV
- GUI: 新規タブ
- 出力ディレクトリ: 既存タブの共通出力とは独立

## 現行実装の事実

### 1. Parquet の保存単位

- JMA: 観測所 × 年 × 月
  - `src/river_meta/rainfall/parquet_store.py`
  - ファイル名: `jma_{station_key}_{year}_{month:02d}.parquet`
- WaterInfo: 観測所 × 年
  - `src/river_meta/rainfall/parquet_store.py`
  - ファイル名: `water_info_{station_key}_{year}.parquet`

### 2. 時刻正規化

- 正規化済み時刻は `observed_at` に保存される
- JMA / WaterInfo の取得時に `normalize_observed_at()` が適用されている
  - `src/river_meta/rainfall/normalizer.py`
- 整理・出力ではさらに `build_hourly_timeseries_dataframe()` で
  - `観測時刻`
  - `1時間雨量(mm)`
  - 欠測を含む時間軸
  を構築している
  - `src/river_meta/rainfall/analysis.py`

### 3. 既存の整理・出力の前提

- 対象は「観測所 × 年」の complete entry
- JMA は12か月揃っている年だけ complete
- WaterInfo は年単位ファイルが存在すれば complete
- 不完全年は generate ではスキップ
  - `src/river_meta/services/rainfall.py`

### 4. GUI の前提

- 既存 GUI は
  - データ取得タブ
  - 整理・出力タブ
  の2系統
- 整理・出力タブは共通出力ディレクトリを前提にしている
  - `src/river_meta/rainfall/gui.py`

## 新機能との整合

### 整合している点

- Parquet 読込基盤は既にある
  - `load_records_parquet()`
  - `load_and_concat_monthly_parquets()`
- 時刻正規化済みデータを取り出せる
- JMA / WaterInfo の差は Parquet 読込層で吸収できる
- 既存整理・出力タブには Parquet スキャン結果テーブルがあり、一覧ベースの選択 UI の方向性と整合する

### そのままでは使えない点

- 既存 generate は「完全年のみ」が前提
- 新機能は「指定日抽出」なので、不完全年でも対象日に Parquet があれば出せる設計にすべき
- 既存 GUI の出力先は共通だが、新機能は独立出力先が必要
- 既存 Excel / グラフは観測所単位出力だが、新機能は日付指定 CSV なので別ユースケース
- 観測所選択は既存取得タブのような外部マスタ検索ではなく、Parquet スキャン結果からの選択にする必要がある

## 実装方針の整理

新機能は `generate` の拡張ではなく、別ユースケースとして切るのが自然。

- service 新設
  - 例: `run_rainfall_parquet_period_export()`
- GUI 新規タブ
  - Parquet 抽出タブ
- 出力
  - CSV のみ

## 想定 UX フロー

1. Parquet ディレクトリを指定
2. スキャン
3. Parquet 一覧から観測所を選択
4. 日付を入力
5. CSV を出力

## 先に決めるべき論点

1. 「日」の指定形式
   - `YYYY-MM-DD` 単日

2. 欠測時間の扱い
   - 欠測時間も 24 行出すか
   - 実測がある時間だけ出すか

3. 出力ファイル名
   - 日付を含める前提で `{source}_{station_code}_{station_name}_{date}.csv` とするか
   - source 付きで統一するか

4. Parquet 一覧 UI の粒度
   - 観測所単位で選ぶか
   - 観測所×年の行から選ぶか
