# 日付境界線（共通設定）仕様の文書化

## 目的
- イベント系グラフの日付境界線を共通設定でON/OFFできる仕様を定義する。
- 日付境界線位置を単一オフセットで調整する仕様を明確化する。

## 対象ファイル
- `docs/dev/requirements/hydrology-graphs-platform.md`
- `docs/dev/architecture/hydrology_graphs.md`
- `docs/dev/reference/hydrology-graphs-platform/style-contract.md`
- `docs/dev/reference/hydrology-graphs-platform/style-json-schema-design.md`
- `src/hydrology_graphs/io/schemas/style_schema_2_0.json`
- `src/hydrology_graphs/io/style_store.py`
- `src/hydrology_graphs/ui/app.py`
- `src/hydrology_graphs/render/plotter.py`
- `tests/hydrology_graphs/test_style_store.py`
- `tests/hydrology_graphs/test_plotter.py`

## 実施内容
- 要件に `graph_styles.<key>.x_axis.date_boundary_line_enabled`（既定 `false`）を追記する。
- 要件に `graph_styles.<key>.x_axis.date_boundary_line_offset_hours`（単一 number）を追記する。
- 境界線の基準位置を `datetime` の `00:00` とし、オフセットで位置調整する仕様を追記する。
- 個別設定フォーム項目（チェックボックス + 単一入力欄）を明記する。
- style contract と JSON Schema 設計にキー・型・既定を追記する。

## 完了条件
- 上記4文書でキー名・型・既定値・描画位置ルールが矛盾なく記載されている。
- 日付境界線表示/オフセットが個別設定として style schema / style_store / UI / 描画で動作する。

## 確認方法
- 各文書で `date_boundary_line_enabled` と境界線位置ルール（基準 `00:00` + 単一オフセット）を目視確認する。
- 各文書で `date_boundary_line_enabled` / `date_boundary_line_offset_hours` が `graph_styles.<key>.x_axis` 配下で記載されていることを目視確認する。
- `uv run pytest -q tests/hydrology_graphs/test_style_store.py tests/hydrology_graphs/test_plotter.py`

## 関連要件 / 関連設計
- 要件: `docs/dev/requirements/hydrology-graphs-platform.md`
- 設計: `docs/dev/architecture/hydrology_graphs.md`

## 完了結果
- `graph_styles.<key>.x_axis.date_boundary_line_enabled` / `date_boundary_line_offset_hours` を schema に追加した。
- style 正規化で上記2キーの既定値補完（`false` / `0.0`）を追加した。
- スタイル個別フォームに `日付境界線表示` / `日付境界線オフセット(時間)` を追加し、ツールチップを付与した。
- 描画処理に日付境界線描画を追加し、`date_boundary_line_enabled=true` 時に `00:00 + offset` で描画するようにした。
