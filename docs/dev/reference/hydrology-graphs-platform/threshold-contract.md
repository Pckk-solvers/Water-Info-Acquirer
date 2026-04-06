# Hydrology Graphs Platform Threshold Contract

## 1. 目的

- 本書は、基準線定義ファイル（CSV/JSON）の入力契約を固定する。
- `requirements.md` の I-02 を実装可能な粒度に落とし込む。

## 2. 対象ファイル形式

- `CSV` または `JSON` を受け付ける。
- 同一の論理スキーマを持つこと。

## 3. 論理スキーマ（v1）

必須項目:

- `source` (string)
- `station_key` (string)
- `graph_type` (string)
- `line_name` (string)
- `value` (number)
- `unit` (string)

任意項目:

- `line_color` (string, hex color)
- `line_style` (string: `solid` | `dashed` | `dotted`)
- `line_width` (number, > 0)
- `label` (string)
- `priority` (integer, default=0)
- `enabled` (boolean, default=true)
- `note` (string)

## 4. 値制約

- `graph_type` は次の6種に限定する。
  - `hyetograph`
  - `hydrograph_discharge`
  - `hydrograph_water_level`
  - `annual_max_rainfall`
  - `annual_max_discharge`
  - `annual_max_water_level`
- `value` は有限実数（NaN/inf 不可）。
- `source` と `station_key` は空文字不可。
- `line_name` は空文字不可。

## 5. 適用キーと重複規則

- 適用キーは `source + station_key + graph_type` とする。
- 同一キーで複数行ある場合は、`priority` 降順で並べる。
- `priority` 同順位は入力順を維持する（CSV/JSONの出現順）。
- 同一キーのレコードを1件に畳み込まず、複数基準線として描画する。
- `enabled=false` は読込時に除外する（描画対象に入らない）。

## 6. CSV 仕様

- UTF-8（BOMあり/なし許容）。
- ヘッダー行必須。
- 区切りは `,`。
- 1行=1基準線。

### 6.1 CSV 例

```csv
source,station_key,graph_type,line_name,value,unit,line_color,line_style,line_width,priority,enabled
water_info,2100000100012,hydrograph_discharge,計画高水流量,1200,m3/s,#DC2626,solid,1.5,10,true
water_info,2100000100012,hydrograph_discharge,避難判断流量,900,m3/s,#EA580C,dashed,1.2,5,true
jma,44132,annual_max_rainfall,計画降雨量,78.5,mm/h,#2563EB,dotted,1.2,10,true
```

## 7. JSON 仕様

- ルートは配列。
- 各要素は 1 基準線オブジェクト。

### 7.1 JSON 例

```json
[
  {
    "source": "water_info",
    "station_key": "2100000100012",
    "graph_type": "hydrograph_water_level",
    "line_name": "計画高水位(HWL)",
    "value": 12.4,
    "unit": "m",
    "line_color": "#DC2626",
    "line_style": "solid",
    "line_width": 1.5,
    "priority": 10,
    "enabled": true
  }
]
```

## 8. バリデーション失敗時の扱い

- 必須項目不足・値制約違反のレコードは読み飛ばす。
- 読み飛ばし件数と理由を警告として結果に残す。
- 全件不正なら `threshold_all_rows_invalid` 警告を返す（ハードエラーにはしない）。

## 9. 互換性方針

- 本書は `v1` 固定とする。
- 将来拡張は任意項目追加で行い、必須項目の削除/名称変更は行わない。
