# Hydrology Graphs Platform Style Contract

Status: target
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

## 3. 各キー配下の必須項目

- `series_color`
- `series_width`
- `series_style`
- `axis`

## 4. 値制約（要点）

- 色は `#RRGGBB` または `#RRGGBBAA`
- サイズ・線幅・DPIは正の値
- `series_style`: `solid|dashed|dotted|dashdot`
- 未知の `graph_styles` キーは警告対象（保存時除外）

## 5. 備考

- 詳細実装の正本は `../current/style-contract.md` を参照する。
- `../archive/style-contract-v1.md` は履歴用（非推奨）。
