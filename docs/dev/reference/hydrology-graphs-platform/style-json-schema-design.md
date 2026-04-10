# Hydrology Graphs Style JSON Schema 設計

## 1. 目的

- `style-contract.md` の契約を、JSON Schema へ落とし込むときの設計正本を定義する。
- 実装（`src/hydrology_graphs/io/style_store.py`）との対応を明確にする。

## 2. スキーマ識別

- 対象: スタイルJSON（Hydrology Graphs Platform）
- 契約バージョン: `schema_version = "2.0"`
- JSON Schema Draft: `2020-12` を採用する
- 実装正本ファイル: `src/hydrology_graphs/io/schemas/style_schema_2_0.json`

## 3. ルート設計

- type: `object`
- required:
  - `schema_version`
  - `graph_styles`
- properties:
  - `schema_version`: const `"2.0"`
  - `graph_styles`: object（必須9キー固定）
  - `display`: object（任意、未指定時は実装側で既定補完）
- additionalProperties: `false`
- 禁止キー:
  - `common`
  - `variants`

## 4. graph_styles 設計

- type: `object`
- required（固定9キー）:
  - `hyetograph:3day`
  - `hyetograph:5day`
  - `hydrograph_discharge:3day`
  - `hydrograph_discharge:5day`
  - `hydrograph_water_level:3day`
  - `hydrograph_water_level:5day`
  - `annual_max_rainfall`
  - `annual_max_discharge`
  - `annual_max_water_level`
- additionalProperties: `true`（未知キーは実装側で警告対象）
- 各キーの値: `#/defs/graphStyle`

## 5. graphStyle 設計（要点）

- required:
  - `series_color`
  - `series_width`
  - `series_style`
  - `axis`
- 主な型制約:
  - 色: `^#(?:[0-9A-Fa-f]{6}|[0-9A-Fa-f]{8})$`
  - 正値: `figure_width`, `figure_height`, `dpi`, `font_size`, `series_width`
  - 非負: `margin.top/right/bottom/left`
  - `series_style`: `solid | dashed | dotted | dashdot`
  - `x_axis.tick_interval_hours`: `exclusiveMinimum: 0`
  - `x_axis.range_margin_rate`: `minimum: 0`（未指定時は実装既定 `0`）
  - `y_axis.tick_count`: 正の整数
- 上記の値制約は JSON Schema 側で定義し、`style_store.py` は主に正規化・互換・エラーコード変換を担う。

## 6. display 設計

- `display.time_display_mode`:
  - enum: `datetime | 24h`
  - 未指定または不正時は実装側で `datetime` に正規化

## 6.1 x_axis 設計（追加）

- `graph_styles.<key>.x_axis.date_boundary_line_enabled`:
  - type: `boolean`
  - 既定: `false`
- `graph_styles.<key>.x_axis.date_boundary_line_offset_hours`:
  - type: `number`
  - 既定: `0.0`
- `graph_styles.<key>.x_axis.data_trim_enabled`:
  - type: `boolean`
  - 既定: `true`
- `graph_styles.<key>.x_axis.data_trim_start_hours`:
  - type: `number`
  - minimum: `0`
  - 既定: `0.0`
- `graph_styles.<key>.x_axis.data_trim_end_hours`:
  - type: `number`
  - minimum: `0`
  - 既定: `0.0`
- `graph_styles.<key>.grid.enabled`:
  - type: `boolean`
  - 既定: `true`
- `graph_styles.<key>.grid.color`:
  - type: `string`（`#RRGGBB`/`#RRGGBBAA`）
- `graph_styles.<key>.grid.width`:
  - type: `number`（`> 0`）
- `graph_styles.<key>.grid.style`:
  - type: `string`
- `graph_styles.<key>.bar.enabled` / `graph_styles.<key>.series.enabled` / `graph_styles.<key>.y_axis.enabled`:
  - type: `boolean`
  - 既定: `true`

## 6.2 フォント/軸オフセット/2軸（追加）

- `graph_styles.<key>.font.title_size`
- `graph_styles.<key>.font.x_label_size`
- `graph_styles.<key>.font.y_label_size`
- `graph_styles.<key>.font.x_tick_size`
- `graph_styles.<key>.font.y_tick_size`
- `graph_styles.<key>.font.legend_size`
- `graph_styles.<key>.axis.x_label_offset`
- `graph_styles.<key>.axis.y_label_offset`
- `graph_styles.<key>.x_axis.tick_label_pad`
- `graph_styles.<key>.y_axis.tick_label_pad`
- `graph_styles.<key>.y2_axis.enabled`
- `graph_styles.<key>.y2_axis.number_format`
- `graph_styles.<key>.y2_axis.tick_label_pad`

## 6.1 日付境界線の描画方針（非スキーマ）

- `date_boundary_line_enabled=true` のときだけ描画する。
- 境界線の基準位置は `datetime` の日付境界 `00:00` とする。
- 実描画位置は `00:00 + graph_styles.<key>.x_axis.date_boundary_line_offset_hours` とする。
- 境界線表示ON/OFFもオフセットも、どちらもグラフ個別設定として扱う。
- `data_trim_enabled=true` のときだけトリムを適用する（`false` の場合は start/end 値があっても未適用）。

## 7. 既定値の扱い

- JSON Schema の `default` は説明用途として記載可能だが、強制力は持たない。
- 実際の既定補完は `style_store.py` の `default_style()` と `load_style()` を正とする。
- 本設計における既定:
  - `display.time_display_mode = "datetime"`
  - `graph_styles.<key>.x_axis.range_margin_rate = 0`（未指定時）

## 8. 実装対応

- スキーマ正本:
  - `src/hydrology_graphs/io/schemas/style_schema_2_0.json`
- 正規化・検証:
  - `src/hydrology_graphs/io/style_store.py`
- 描画反映:
  - `src/hydrology_graphs/render/plotter.py`

## 8.1 UIグループとスキーマキーの対応方針

- UIのグループ（文字設定 / 軸設定 / グリッド・境界線 / 系列・棒設定 / スケール設定 / データ範囲設定）は表示上の分類であり、スキーマの入れ子構造は変更しない。
- スキーマ正本は引き続き `graph_styles.<graph_key>.*` 配下のキーで維持する。
- 将来拡張時も、まず既存キーを追加し、UI側でグルーピングを定義する。グループ見出しの表示は任意とする。
- 互換性維持のため、`groups.*` のようなUI専用キーはスキーマに追加しない。

## 9. JSON Schema ひな形（抜粋）

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://example.local/hydrology-graphs/style-schema-2.0.json",
  "type": "object",
  "additionalProperties": false,
  "required": ["schema_version", "graph_styles"],
  "properties": {
    "schema_version": { "const": "2.0" },
    "display": {
      "type": "object",
      "additionalProperties": true,
      "properties": {
        "time_display_mode": { "type": "string", "enum": ["datetime", "24h"], "default": "datetime" }
      }
    },
    "graph_styles": {
      "type": "object",
      "additionalProperties": true,
      "required": [
        "hyetograph:3day",
        "hyetograph:5day",
        "hydrograph_discharge:3day",
        "hydrograph_discharge:5day",
        "hydrograph_water_level:3day",
        "hydrograph_water_level:5day",
        "annual_max_rainfall",
        "annual_max_discharge",
        "annual_max_water_level"
      ],
      "properties": {
        "hyetograph:3day": { "$ref": "#/$defs/graphStyle" }
      }
    }
  },
  "$defs": {
    "graphStyle": {
      "type": "object",
      "required": ["series_color", "series_width", "series_style", "axis"],
      "properties": {
        "series_color": { "type": "string", "pattern": "^#(?:[0-9A-Fa-f]{6}|[0-9A-Fa-f]{8})$" },
        "series_width": { "type": "number", "exclusiveMinimum": 0 },
        "series_style": { "type": "string", "enum": ["solid", "dashed", "dotted", "dashdot"] },
        "x_axis": {
          "type": "object",
          "properties": {
            "tick_interval_hours": { "type": "number", "exclusiveMinimum": 0 },
            "range_margin_rate": { "type": "number", "minimum": 0, "default": 0 },
            "data_trim_enabled": { "type": "boolean", "default": true },
            "data_trim_start_hours": { "type": "number", "minimum": 0, "default": 0.0 },
            "data_trim_end_hours": { "type": "number", "minimum": 0, "default": 0.0 },
            "date_boundary_line_enabled": { "type": "boolean", "default": false },
            "date_boundary_line_offset_hours": { "type": "number", "default": 0.0 }
          }
        }
      }
    }
  }
}
```
