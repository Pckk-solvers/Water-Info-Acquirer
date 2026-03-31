# Hydrology Graphs Platform Style Contract

## 1. 目的

- 本書は、スタイル JSON の入力/保存契約を固定する。
- `requirements.md` の I-01 を実装可能な粒度に落とし込む。

## 2. ルート構造（v1）

必須項目:

- `schema_version` (string, 必須, `1.0`)
- `common` (object, 必須)
- `graph_styles` (object, 必須)

## 3. common ブロック

必須項目:

- `font_family` (string)
- `font_size` (number)
- `figure_width` (number)
- `figure_height` (number)
- `margin` (object: `top`, `right`, `bottom`, `left` all number)
- `legend` (object: `enabled` boolean, `position` string)

任意項目:

- `aspect_mode` (string: `fixed` | `auto`, default=`fixed`)
- `dpi` (integer, default=120)
- `grid` (object: `enabled` boolean, `color` string, `style` string, `alpha` number)
- `background_color` (string)
- `padding` (object: `outer` object with `top`, `right`, `bottom`, `left`)
- `font` (object: `title_size`, `label_size`, `tick_size`)
- `legend.fixed_anchor` (object with `x`, `y`)
- `export` (object: `transparent_background` boolean, default=false)

## 4. graph_styles ブロック

- キーは以下6種を必須とする。
  - `hyetograph`
  - `hydrograph_discharge`
  - `hydrograph_water_level`
  - `annual_max_rainfall`
  - `annual_max_discharge`
  - `annual_max_water_level`

各グラフの必須項目:

- `series_color` (string)
- `series_width` (number)
- `series_style` (string: `solid` | `dashed` | `dotted`)
- `axis` (object: `x_label`, `y_label`)

任意項目:

- `bar_color` (string)
- `secondary_series_color` (string)
- `invert_y_axis` (boolean)
- `show_markers` (boolean)
- `title` (object: `template` string)
- `x_axis` (object: `date_format` string, `tick_rotation` number, `tick_interval_hours` number, `label_align` string)
- `y_axis` (object: `number_format` string, `min` number, `max` number, `tick_step` number, `tick_count` integer)
- `bar` (object: `width` number)
- `threshold` (object: `label_enabled` boolean, `label_offset` number, `label_font_size` number, `zorder` number)
- `series` (object: `zorder` number)

## 5. 値制約

- `schema_version` は `1.0` のみ受理。
- 色は `#RRGGBB` または `#RRGGBBAA`。
- 線幅・フォントサイズ・図サイズは正の値。
- 未知の `graph_styles` キーは警告対象（読み飛ばし）。
- `x_axis.tick_interval_hours` は正の値。
- `bar.width` は正の値。
- `y_axis.tick_step` は正の値。
- `y_axis.tick_count` は正の整数。
- `export.transparent_background` は boolean。

## 6. 読込・保存時の動作

- 読込時:
  - 必須キー不足はエラー。
  - 任意キー不足は既定値補完。
  - 未知キーは警告して保持しない（v1 実装方針）。
  - 旧キー互換入力は正規キーへ変換して受理する（下記 6.1）。
- 保存時:
  - `schema_version` が `1.0` でない場合は保存拒否。
  - GUI 編集結果は必須キーを補完して保存。
  - 保存時は正規キー形式のみ出力する。

### 6.1 旧キー互換（読込時のみ）

- `title_template` が存在し `title.template` が未指定の場合:
  - `title.template = title_template` として扱う。
- `title_template` と `title.template` が両方ある場合:
  - `title.template` を優先する。

## 7. 例（v1）

```json
{
  "schema_version": "1.0",
  "common": {
    "font_family": "Yu Gothic UI",
    "font_size": 11,
    "figure_width": 12,
    "figure_height": 6,
    "aspect_mode": "fixed",
    "margin": { "top": 0.08, "right": 0.04, "bottom": 0.12, "left": 0.08 },
    "padding": { "outer": { "top": 0.08, "right": 0.04, "bottom": 0.12, "left": 0.08 } },
    "font": { "title_size": 14, "label_size": 12, "tick_size": 10 },
    "legend": { "enabled": true, "position": "upper right", "fixed_anchor": { "x": 1.0, "y": 1.0 } },
    "dpi": 120,
    "grid": { "enabled": true, "color": "#CBD5E1", "style": "--", "alpha": 0.7 },
    "export": { "transparent_background": false }
  },
  "graph_styles": {
    "hyetograph": {
      "series_color": "#2563EB",
      "series_width": 1.2,
      "series_style": "solid",
      "axis": { "x_label": "時刻", "y_label": "雨量 (mm/h)" },
      "bar_color": "#60A5FA",
      "invert_y_axis": true,
      "title": { "template": "{station_name} ハイエトグラフ" },
      "x_axis": { "date_format": "%m/%d %H:%M", "tick_rotation": 45, "tick_interval_hours": 6, "label_align": "center" },
      "y_axis": { "number_format": "plain", "tick_count": 6 },
      "bar": { "width": 0.8 },
      "threshold": { "label_enabled": true, "label_offset": 0.02, "label_font_size": 10, "zorder": 3 },
      "series": { "zorder": 2 }
    },
    "hydrograph_discharge": {
      "series_color": "#0F766E",
      "series_width": 1.5,
      "series_style": "solid",
      "axis": { "x_label": "時刻", "y_label": "流量 (m3/s)" },
      "title": { "template": "{station_name} ハイドログラフ（流量）" },
      "x_axis": { "date_format": "%m/%d %H:%M", "tick_rotation": 45, "tick_interval_hours": 6, "label_align": "center" },
      "y_axis": { "number_format": "comma", "min": 0, "tick_step": 500, "tick_count": 8 },
      "threshold": { "label_enabled": true, "label_offset": 0.03, "label_font_size": 10, "zorder": 3 },
      "series": { "zorder": 2 }
    },
    "hydrograph_water_level": {
      "series_color": "#7C3AED",
      "series_width": 1.5,
      "series_style": "solid",
      "axis": { "x_label": "時刻", "y_label": "水位 (m)" },
      "title": { "template": "{station_name} 水位波形" },
      "x_axis": { "date_format": "%m/%d %H:%M", "tick_rotation": 45, "tick_interval_hours": 6, "label_align": "center" },
      "y_axis": { "number_format": "plain", "tick_count": 7 },
      "threshold": { "label_enabled": true, "label_offset": 0.05, "label_font_size": 10, "zorder": 3 },
      "series": { "zorder": 2 }
    },
    "annual_max_rainfall": {
      "series_color": "#1D4ED8",
      "series_width": 1.2,
      "series_style": "solid",
      "axis": { "x_label": "年", "y_label": "年最大雨量" },
      "bar_color": "#60A5FA",
      "title": { "template": "{station_name} 年最大雨量" },
      "x_axis": { "tick_rotation": 90, "label_align": "center" },
      "y_axis": { "number_format": "plain", "min": 0, "tick_count": 8 },
      "bar": { "width": 0.8 },
      "series": { "zorder": 2 }
    },
    "annual_max_discharge": {
      "series_color": "#0F766E",
      "series_width": 1.2,
      "series_style": "solid",
      "axis": { "x_label": "年", "y_label": "年最大流量" },
      "bar_color": "#34D399",
      "title": { "template": "{station_name} 年最大流量" },
      "x_axis": { "tick_rotation": 90, "label_align": "center" },
      "y_axis": { "number_format": "comma", "min": 0, "tick_step": 500, "tick_count": 8 },
      "bar": { "width": 0.8 },
      "series": { "zorder": 2 }
    },
    "annual_max_water_level": {
      "series_color": "#6D28D9",
      "series_width": 1.2,
      "series_style": "solid",
      "axis": { "x_label": "年", "y_label": "年最高水位" },
      "bar_color": "#A78BFA",
      "title": { "template": "{station_name} 年最高水位" },
      "x_axis": { "tick_rotation": 90, "label_align": "center" },
      "y_axis": { "number_format": "plain", "tick_count": 8 },
      "bar": { "width": 0.8 },
      "series": { "zorder": 2 }
    }
  }
}
```

## 8. 互換性方針

- 本書は `v1` 固定。
- `schema_version` を更新する拡張は後方互換を意識して追加キー中心で行う。

## 9. 設定項目の調整対象（実務向けメモ）

### 9.1 common

- `figure_width`, `figure_height`: 出力画像の横幅・縦幅を調整する。
- `aspect_mode`: 比率固定（`fixed`）か自動（`auto`）かを切り替える。
- `margin`, `padding.outer`: 図の外周余白を調整し、複数画像の揃いを作る。
- `font_family`, `font.*`: タイトル・軸ラベル・目盛の文字サイズとフォントを揃える。
- `legend.position`, `legend.fixed_anchor`: 凡例の位置を固定し、比較時のブレを抑える。
- `grid.*`: グリッドの有無、線種、濃さを調整する。
- `export.transparent_background`: 背景透過の有無を切り替える（通常は `false`）。

### 9.2 graph_styles

- `series_color`, `series_width`, `series_style`: 主系列の色・線幅・線種を調整する。
- `bar_color`, `bar.width`: 棒グラフの見た目と太さを調整する。
- `secondary_series_color`: 累加雨量など副系列の色を調整する。
- `title.template`: タイトル文言のテンプレートを調整する。
- `x_axis.date_format`: 日時軸の表示形式（例 `%m/%d %H:%M`）を調整する。
- `x_axis.tick_rotation`: X軸ラベルの回転角を調整する。
- `x_axis.tick_interval_hours`: 時系列グラフのX軸目盛間隔（時間）を調整する。
- `x_axis.label_align`: X軸ラベルの配置感（中央寄せなど）を揃える。
- `y_axis.number_format`: 数値表示形式（桁区切りなど）を調整する。
- `y_axis.min`, `y_axis.max`: Y軸表示範囲を固定し、図間比較をしやすくする。
- `y_axis.tick_step`, `y_axis.tick_count`: Y軸目盛の刻み・本数を揃える。
- `invert_y_axis`: ハイエトグラフなどでY軸反転を有効化する。
- `show_markers`: 折れ線のマーカー表示有無を切り替える。
- `threshold.label_enabled`: 基準線ラベルの表示有無を切り替える。
- `threshold.label_offset`: 基準線ラベルの線からのオフセット量を調整する。
- `threshold.label_font_size`: 基準線ラベルの文字サイズを調整する。
- `series.zorder`, `threshold.zorder`: 系列と基準線の前後関係を調整する。
