# 雨量・水位流量 時系列 Parquet 共通化 詳細設計

## 1. 目的

- JMA / WaterInfo の時系列データ保存を共通スキーマへ統一する。
- 時刻の内部表現を `observed_at`（naive JST）へ固定する。
- 3日/5日グラフ再生成で使う読込入口を source 非依存 API に統一する。

## 2. 変更対象

- `src/river_meta/rainfall/domain/`
  - 共通時系列レコード用モデルの追加（または既存モデル拡張）
- `src/river_meta/rainfall/sources/`
  - JMA / WaterInfo の保存前変換レイヤ追加
- `src/river_meta/rainfall/storage/`
  - 共通スキーマ Parquet の保存・読込 API 追加
  - 旧 `rainfall_mm` スキーマからの読込互換層追加
- `src/river_meta/rainfall/services/`
  - グラフ再生成用の共通読込入口を追加
- `tests/river_meta/`
  - 変換・保存読込・互換・時刻整合のテスト追加

## 3. 設計方針

### 3.1 時刻方針（確定）

- `observed_at` は timezone なし（naive）の JST で保持する。
- 保存時の時刻値は `00:00〜23:00` を正とする。
- `1〜24時` 表記は表示変換でのみ扱う。
- source 差分（例: JMA 1時間値補正）は保存前正規化で吸収する。

### 3.2 スキーマ方針（確定）

必須列:

- `source`
- `station_key`
- `station_name`
- `observed_at`
- `metric`
- `value`
- `unit`
- `interval`
- `quality`

仕様:

- `metric`: `rainfall`, `water_level`, `discharge` を最低限サポート。
- `value`: 観測元の1次値（非派生値）のみ保存。
- 欠測: `value=null` かつ `quality=missing`。

### 3.3 粒度方針（確定）

- 物理保存粒度は当面現行維持。
  - JMA: 観測所×年×月
  - WaterInfo: 観測所×年
- 利用側は論理粒度で統一する。
  - `station_key + metric + interval + time_range`
- 読込 API が物理粒度差を吸収する。

### 3.4 非スコープ（確定）

- JSON エクスポート仕様
- 観測所メタ（station master）の新規整備
- 任意列の実装追加

## 4. データモデル設計

### 4.1 共通レコード（保存対象）

想定 dataclass（名称は実装側で確定）:

- `source: Literal["jma", "water_info"]`
- `station_key: str`
- `station_name: str`
- `observed_at: datetime`（naive JST）
- `metric: Literal["rainfall", "water_level", "discharge"]`
- `value: float | None`
- `unit: str`
- `interval: Literal["10min", "1hour", "1day"]`
- `quality: str`（`normal` / `missing` を最低保証）

### 4.2 旧スキーマ互換

旧 `rainfall_mm` スキーマを読込時に次へ写像する。

- `metric = "rainfall"`
- `value = rainfall_mm`
- `unit = "mm"`
- 他列は同名を継承

## 5. 保存前変換設計

### 5.1 入口

新規関数（名称案）:

- `normalize_to_storage_record(source, raw_record, context) -> UnifiedRecord`

責務:

- source 別 raw の差分吸収
- 時刻正規化（naive JST）
- `metric/value/unit/interval/quality` の確定

### 5.2 JMA 変換

- 既存の時刻補正ロジック（1時間値 -1h）をここへ集約。
- 出力 `metric` は現段階では `rainfall`。

### 5.3 WaterInfo 変換

- `mode_type` に応じて `metric/unit` を決定。
  - `U -> rainfall/mm`
  - `S -> water_level/m`
  - `R -> discharge/m3/s`
- `station_id` を `station_key` とする。

## 6. 保存・読込 API 設計

### 6.1 保存 API

新規関数（名称案）:

- `save_unified_records_parquet(records, output_path) -> Path`

仕様:

- 必須列のみ書き出す。
- `observed_at` を `datetime64[ns]` として保存。
- 空書込は作成しない（既存運用に合わせる）。

### 6.2 読込 API

新規関数（名称案）:

- `load_unified_timeseries(output_dir, source, station_key, metric, interval, start_at, end_at) -> DataFrame`

仕様:

- source ごとの物理ファイル粒度差を内部吸収。
- 旧新スキーマ混在時は新優先。
- 同一キー衝突時は旧行を採用しない。
- 新スキーマが存在しない範囲のみ旧スキーマをフォールバック。

### 6.3 キー衝突判定

同一キー:

- `source, station_key, observed_at, metric, interval`

運用:

- 新スキーマ側を採用。
- 旧スキーマ側はドロップ。

## 7. グラフ再生成連携設計

- 3日/5日グラフで使うデータ取得は `load_unified_timeseries(...)` を唯一入口にする。
- グラフ描画直前で `hour = observed_at.hour + 1` を適用する。
- 描画ロジックは `observed_at` を 00〜23 のまま扱い、軸ラベルのみ変換する。

## 8. エラー設計

- 必須列欠落: `ValueError`（列名を明記）
- 時刻解釈不能: 該当行を欠測扱いにするか、件数閾値超過時はエラー（実装時に統一）
- 未対応 `metric` / `interval`: `ValueError`
- 対象期間に有効データなし: 空 DataFrame を返却

## 9. テスト設計

### 9.1 時刻

- JMA / WaterInfo とも保存値が naive JST 00〜23 になる。
- `1〜24時` 変換が保存時に混入しない。

### 9.2 変換

- `mode_type` ごとに `metric/unit` が正しく写像される。
- `value` が非派生値で保存される。

### 9.3 読込

- JMA（月）/WaterInfo（年）で同一 API 呼び出しが成立する。
- 期間フィルタ（3日/5日）で期待範囲のみ返る。

### 9.4 互換

- 旧のみ: 読込可能。
- 新のみ: 読込可能。
- 旧新混在: 新優先・旧フォールバックが機能する。

## 10. 実装ステップ

1. 共通レコードモデル追加（保存列定義を確定）
2. 保存前変換レイヤ追加（JMA / WaterInfo）
3. 保存 API 追加
4. 読込 API 追加（旧互換含む）
5. グラフ再生成入口を共通読込 API に切替
6. テスト追加・既存回帰確認

## 11. 受け入れ基準

- 必須列のみで新スキーマ Parquet が保存される。
- `observed_at` が naive JST 固定で一貫する。
- 物理粒度差を利用側が意識せず抽出できる。
- 旧新混在時に新スキーマが優先される。
- 3日/5日グラフ再生成で時刻軸表示が崩れない。
