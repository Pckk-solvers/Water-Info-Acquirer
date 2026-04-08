# Hydrology Graphs Platform Style Contract (Current)

Schema Version: 2.0

## 1. ルート構造

必須:
- `schema_version`: `"2.0"`
- `graph_styles`: object

任意:
- `display`: object
  - `time_display_mode`: `datetime|24h`（未指定時 `datetime`）

禁止:
- `common`
- `variants`

## 2. graph_styles キー（必須9種）

- `hyetograph:3day`
- `hyetograph:5day`
- `hydrograph_discharge:3day`
- `hydrograph_discharge:5day`
- `hydrograph_water_level:3day`
- `hydrograph_water_level:5day`
- `annual_max_rainfall`
- `annual_max_discharge`
- `annual_max_water_level`

## 3. 各キー配下の代表項目

- `figure_width`, `figure_height`, `dpi`
- `font_family`, `font_size`
- `background_color`
- `margin`, `legend`, `grid`, `font`, `export`
- `series_color`, `series_width`, `series_style`
- `axis.x_label`, `axis.y_label`
- 任意: `x_axis`, `y_axis`, `bar`, `threshold`, `series`
  - `x_axis.range_margin_rate` を指定した場合は X軸データ範囲の余白率として扱う（例: `0.05` で両端5%）
  - `x_axis.date_boundary_line_enabled` を指定した場合は日付境界線表示のON/OFFとして扱う
  - `x_axis.date_boundary_line_offset_hours` を指定した場合は日付境界線の位置オフセット（時間）として扱う

## 4. 値制約（要点）

- 色は `#RRGGBB` または `#RRGGBBAA`
- サイズ/線幅/DPIは正の値
- `series_style`: `solid|dashed|dotted|dashdot`
- 未知の `graph_styles` キーは警告対象（保存時除外）
- `x_axis.range_margin_rate` は `0` 以上の数値のみ許可
- `x_axis.range_margin_rate` 未指定時は `0` を既定として扱う
- `x_axis.date_boundary_line_enabled` は boolean のみ許可
- `x_axis.date_boundary_line_enabled` 未指定時は `false` を既定として扱う
- `x_axis.date_boundary_line_offset_hours` は number のみ許可
- `x_axis.date_boundary_line_offset_hours` 未指定時は `0.0` を既定として扱う

## 5. 備考

実装正本:
- `src/hydrology_graphs/io/schemas/style_schema_2_0.json`
- `src/hydrology_graphs/io/style_store.py`
- `src/hydrology_graphs/render/plotter.py`

