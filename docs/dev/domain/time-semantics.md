# 時間意味と境界ルール

Status: current

## 1. 正式ルール

- `water_info` の `S` / `R` は瞬間値として扱う。
- `water_info` の `U` は区間値として扱う。
- JMA の雨量は区間値として扱う。
- 内部表現は `datetime` を正とし、`display_dt` / `display_at` の常設保持はしない。

## 2. 時刻列の意味

### 瞬間値

- `observed_at`: 実観測時刻
- `period_start_at`: `null`
- `period_end_at`: `null`

### 区間値

- `observed_at`: 区間終端時刻
- `period_start_at`: 区間開始時刻
- `period_end_at`: 区間終了時刻

## 3. 境界の扱い

- `request_window`: ユーザーが欲しい論理期間
- `fetch_window`: 境界レコードを埋めるために広げた取得期間
- `publish_window`: 最終的に出力へ反映する期間

区間値の出力判定は `period_end_at` を基準にし、瞬間値は `observed_at` を基準にする。

## 4. 24時相当の扱い

- 24時相当は翌日 `00:00:00` として保持する。
- `23:59:59.999999` のような擬似終端は使わない。

## 5. 関連実装

- `src/river_meta/rainfall/domain/models.py`
- `src/river_meta/rainfall/domain/normalizer.py`
- `src/river_meta/rainfall/storage/parquet_store.py`
- `src/hydrology_graphs/io/parquet_store.py`
- `src/water_info/entry.py`
