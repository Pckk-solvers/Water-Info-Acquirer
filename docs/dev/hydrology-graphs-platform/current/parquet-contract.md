# Hydrology Graphs Platform Parquet Contract

## 1. 目的
- 本ドキュメントは、Hydrology Graphs Platform が入力として受け付ける Parquet の契約（列・型・値・時刻ルール）を定義する。
- 現行実装（JMA / water_info の GUI 出力）との整合を維持しつつ、将来拡張時の互換基準とする。

## 2. 適用範囲
- 対象ソース: `jma`, `water_info`
- 対象用途:
  - ハイエトグラフ（rainfall）
  - ハイドログラフ（water_level, discharge）
  - 年最大グラフ（rainfall, water_level, discharge）

## 3. 必須カラム（共通時系列）
以下 9 列を必須とする。

1. `source`
2. `station_key`
3. `station_name`
4. `observed_at`
5. `metric`
6. `value`
7. `unit`
8. `interval`
9. `quality`

現行実装の根拠:
- `src/jma_rainfall_pipeline/exporter/parquet_exporter.py` の `_UNIFIED_COLUMNS`
- `src/water_info/entry.py` の `_UNIFIED_COLUMNS`

## 4. カラム仕様
- `source`: 文字列
  - 許容値: `jma`, `water_info`
- `station_key`: 文字列
  - `jma`: `<prec_no>_<block_no>`（例: `48_47618`）
  - `water_info`: 観測所コード（例: `304061284408080`）
- `station_name`: 文字列
  - 原則設定する（`jma` / `water_info` ともに観測所名を格納）
  - 取得失敗時は空文字を許容（フォールバック）
- `observed_at`: 日時
  - JST 固定（タイムゾーン列は持たない）
  - 文字列/日時の入力は `pd.to_datetime(..., errors="coerce")` で解釈可能であること
- `metric`: 文字列
  - 許容値: `rainfall`, `water_level`, `discharge`
- `value`: 数値または null
- `unit`: 文字列
  - `rainfall` -> `mm`
  - `water_level` -> `m`
  - `discharge` -> `m3/s`
- `interval`: 文字列
  - 許容値: `10min`, `1hour`, `1day`
- `quality`: 文字列
  - 許容値（現行）: `normal`, `missing`

## 5. 現行実装での metric/unit マッピング
### JMA
- `source`: `jma`
- `station_name`: `fetch_station_codes(prec_no)` から `block_no` で解決して設定
- `metric`: `rainfall`
- `unit`: `mm`
- `interval`: `daily -> 1day`, `hourly -> 1hour`, `10min -> 10min`

### water_info
- `source`: `water_info`
- `mode_type=S` -> `metric=water_level`, `unit=m`
- `mode_type=R` -> `metric=discharge`, `unit=m3/s`
- `mode_type=U` -> `metric=rainfall`, `unit=mm`
- `interval`: 時間データは `1hour`、日データは `1day`

## 6. 時刻ルール
- `observed_at` は JST で扱う（絶対ルール）。
- 保持時に timezone 列は持たない。
- 24:00 相当などの端数時刻は、保存前に実装側で正規化する。
- 1時間値は Hydro時刻（`00:00`〜`23:00`）へ正規化して保持する。
  - 1時値は当日 `00:00`
  - 24時値（翌日 `00:00` 相当）は当日 `23:00`
  - 旧JMA由来の `23:59:59.999999` は正規化対象として扱う

## 7. グラフ機能側の最低入力条件
### ハイエトグラフ
- `metric=rainfall`
- `interval=1hour`
- 基準日を含む 3日または5日の連続時系列

### ハイドログラフ
- `metric in (water_level, discharge)`
- `interval=1hour`
- 基準日を含む 3日または5日の連続時系列

### 年最大グラフ
- `metric in (rainfall, water_level, discharge)`
- 年最大算出に十分な長期間データ
- 現行実装は「年最大が10年以上」必須（`has_min_years(..., 10)`）

## 8. ファイル名（現行実装の慣例）
- JMA:
  - `jma_<station_key>_<interval>_<startYYYYMM>_<endYYYYMM>.parquet`
- water_info:
  - `water_info_<station_key>_<metric>_<interval>_<startYYYYMM>_<endYYYYMM>.parquet`

注: ファイル名は識別補助であり、機能側は中身（列）を一次情報として判定する。

## 9. 検証ポリシー
- 読み込み時に必須列不足・許容外値・時刻解釈失敗を検出した場合は対象をスキップし、理由をユーザーに表示する。
- 可能な限り「全体失敗」ではなく「対象単位での部分成功」を優先する。
- ディレクトリスキャンは並列実行し、ファイルサイズ+mtimeのフィンガープリントで結果キャッシュを行う。

## 10. 変更管理
- カラム追加・許容値変更・時刻ルール変更が発生する場合は、本ファイル更新を必須とする。
- 破壊的変更時はバージョン番号（例: `contract_version`）の導入を検討する。
