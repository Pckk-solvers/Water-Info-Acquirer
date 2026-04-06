# Hydrology Graphs Platform Style Contract (Current)

Schema Version: 2.0

## 1. ルート構造

必須:
- `schema_version`: `"2.0"`
- `graph_styles`: object

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

## 4. 値制約（要点）

- 色は `#RRGGBB` または `#RRGGBBAA`
- サイズ/線幅/DPIは正の値
- `series_style`: `solid|dashed|dotted|dashdot`
- 未知の `graph_styles` キーは警告対象（保存時除外）

## 5. 備考

実装正本:
- `src/hydrology_graphs/io/style_store.py`
- `src/hydrology_graphs/render/plotter.py`

