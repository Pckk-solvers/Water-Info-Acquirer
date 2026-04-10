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
- `margin`, `legend`, `grid`, `font`, `export`
- `series_color`, `series_width`, `series_style`
- `axis.x_label`, `axis.y_label`
- 任意: `x_axis`, `y_axis`, `bar`, `threshold`, `series`
  - `grid.enabled` を指定した場合はグリッド行全体のON/OFFとして扱う
  - `x_axis.range_margin_rate` を指定した場合は X軸データ範囲の余白率として扱う（例: `0.05` で両端5%）
  - `x_axis.data_trim_enabled` を指定した場合は表示データ範囲トリムのON/OFFとして扱う
  - `x_axis.data_trim_start_hours` / `x_axis.data_trim_end_hours` を指定した場合は描画前データの先頭/末尾トリム（時間）として扱う
  - `x_axis.date_boundary_line_enabled` を指定した場合は日付境界線表示のON/OFFとして扱う
  - `x_axis.date_boundary_line_offset_hours` を指定した場合は日付境界線の位置オフセット（時間）として扱う
  - `bar.enabled` を指定した場合は棒描画（ハイエト/年最大）のON/OFFとして扱う
  - `series.enabled` を指定した場合は系列描画（主にハイドロ折れ線）のON/OFFとして扱う
  - `y_axis.enabled` を指定した場合はY軸設定（max/tick_step/number_format）の適用ON/OFFとして扱う
  - `y_axis.number_format` は `plain|comma|percent` を受け付ける（ハイエトグラフでは左右Y軸に同一適用）

## 4. 値制約（要点）

- 色は `#RRGGBB` または `#RRGGBBAA`
- サイズ/線幅/DPIは正の値
- `series_style`: `solid|dashed|dotted|dashdot`
- 未知の `graph_styles` キーは警告対象（保存時除外）
- `x_axis.range_margin_rate` は `0` 以上の数値のみ許可
- `x_axis.range_margin_rate` 未指定時は `0` を既定として扱う
- `grid.enabled` は boolean のみ許可（未指定時 `true`）
- `x_axis.data_trim_enabled` は boolean のみ許可（未指定時 `true`）
- `x_axis.data_trim_start_hours` / `x_axis.data_trim_end_hours` は `0` 以上の数値のみ許可
- `x_axis.data_trim_start_hours` / `x_axis.data_trim_end_hours` 未指定時は `0.0` を既定として扱う
- `bar.enabled` / `series.enabled` / `y_axis.enabled` は boolean のみ許可（未指定時 `true`）
- `x_axis.date_boundary_line_enabled` は boolean のみ許可
- `x_axis.date_boundary_line_enabled` 未指定時は `false` を既定として扱う
- `x_axis.date_boundary_line_offset_hours` は number のみ許可
- `x_axis.date_boundary_line_offset_hours` 未指定時は `0.0` を既定として扱う
- 背景色は固定（白）で、スタイル項目としては扱わない

## 5. 備考

実装正本:
- `src/hydrology_graphs/io/schemas/style_schema_2_0.json`
- `src/hydrology_graphs/io/style_store.py`
- `src/hydrology_graphs/render/plotter.py`

## 6. UIグルーピング方針（拡張）

細かい調整項目を増やす際は、グラフ種別ベースではなく役割ベースでグループ化する。
ただし、利用者にグループ名を見せることは必須ではなく、ラベル名と配置で分かることを優先する。

- `文字設定`
  - 代表項目: `font_family`, `font_size`, `font_weight`, `font_color`
  - 対象: タイトル、X/Y軸ラベル、目盛、凡例、注記、基準線ラベル
- `軸設定`
  - 代表項目: `x_axis.range_margin_rate`, `x_axis.tick_rotation`, `x_axis.label_offset`, `y_axis.label_offset`, `spine.width`
- `グリッド・境界線`
  - 代表項目: `grid.x_enabled`, `grid.y_enabled`, `grid.color`, `grid.width`, `x_axis.date_boundary_line_enabled`, `x_axis.date_boundary_line_offset_hours`
- `系列・棒設定`
  - 代表項目: `series_color`, `series_width`, `series_style`, `bar_color`, `bar.width`, `bar.edge_width`
- `スケール設定`
  - 代表項目: `y_axis.max`, `y_axis.tick_step`, `y_axis.number_format`（必要時は左右軸を個別化）
- `データ範囲設定`
  - 代表項目: `x_axis.data_trim_enabled`, `x_axis.data_trim_start_hours`, `x_axis.data_trim_end_hours`

### 6.1 フォーム配置ルール

- 1行1設定を原則とする。
- 単一入力で足りる行は直接入力（Entry/Combobox/Checkbutton）を使う。
- 色・線種・太さなど複数入力を伴う行は `設定...` ダイアログ（詳細設定行）に寄せる。
- ON/OFFが必要な行は、左列チェックボックスで有効化し、右列で値を編集する。
- 見出しを省略しても配置ルールが崩れないよう、開始位置・列構成・余白を統一する。

### 6.2 グループの運用制約

- 現行スキーマは `graph_styles.<graph_key>` 配下を正本とし、`groups` の新規導入は行わない。
- UI上のグループ表示は任意とし、必要な場合のみ既存キーの再配置で実現する（キー互換性を維持）。

### 6.3 UI廃止対象（段階移行）

以下は UI から段階的に廃止する。

- `series_color` / `series_width` / `series_style` の単独入力行（`系列設定` 詳細設定行へ統合）
- `bar_color` / `bar.width` の単独入力行（`棒設定` 詳細設定行へ統合）
- `font_size`（基本フォントサイズ。個別フォントサイズ導入後はUI非表示）
- `grid.enabled`（`grid.x_enabled` / `grid.y_enabled` へ移行）

互換方針:
- 既存JSONの読込では旧キーを許容する。
- UIは新しい編集導線のみ提示し、旧キーは直接編集対象にしない。

### 6.4 ハイエトグラフ2軸設定方針

- ハイエトグラフ（`hyetograph:*`）は2軸（左=時間雨量、右=累積雨量）を個別設定可能とする。
- 左右軸の上限・刻み・数値形式は独立して持てる設計を優先する。
- 行数増加を避けるため、UIは「Y軸設定（左）」「Y軸設定（右）」の詳細設定行（設定ダイアログ）を基本とする。

### 6.5 実装済みキー（2026-04-09 反映）

- 文字サイズ（個別）
  - `font.title_size`
  - `font.x_label_size`
  - `font.y_label_size`
  - `font.x_tick_size`
  - `font.y_tick_size`
  - `font.legend_size`
- 軸位置
  - `axis.x_label_offset`
  - `axis.y_label_offset`
  - `x_axis.tick_label_pad`
  - `y_axis.tick_label_pad`
  - `y2_axis.tick_label_pad`
- グリッド線詳細
  - `grid.color`
  - `grid.width`
  - `grid.style`
- ハイエト右軸
  - `y2_axis.enabled`
  - `y2_axis.max`
  - `y2_axis.tick_step`
  - `y2_axis.number_format`

### 6.6 UI廃止反映（2026-04-09）

- 単独行を廃止:
  - `series_color` / `series_width` / `series_style`
  - `bar_color` / `bar.width`
- UI表示を廃止:
  - `font_size`（基本フォントサイズ）
  - `grid.enabled`（全体ON/OFF）
- 互換読込は維持し、既存JSONは読める状態とする。

