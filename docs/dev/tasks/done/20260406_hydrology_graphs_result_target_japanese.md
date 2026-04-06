# 結果対象の日本語表示化

## 目的
- Hydrology Graphs の結果一覧で、`対象` 列に内部IDではなく日本語の説明ラベルを表示する。
- `高幡橋（気象庁:111） / ハイドログラフ（水位） / 2025-01-02 / 3日窓` の形式にそろえる。
- 内部の `target_id` は保持し、ダブルクリックや出力先参照では従来どおり使う。

## 対象ファイル
- `docs/dev/requirements/hydrology-graphs-platform.md`
- `docs/dev/architecture/hydrology_graphs.md`
- `src/hydrology_graphs/ui/app.py`
- `src/hydrology_graphs/ui/execute_actions.py`
- `src/hydrology_graphs/ui/event_handlers.py`
- `src/hydrology_graphs/ui/view_models.py`
- `tests/hydrology_graphs/test_event_handlers.py`
- `tests/hydrology_graphs/test_ui_support.py`

## 実施内容
- precheck / batch の結果一覧で共通の日本語ラベル整形を使う。
- 観測所名、source 表記、グラフ種別、基準日、窓を見やすく連結する。
- 内部IDと表示ラベルを分離する。

## 結果
- 結果一覧の `対象` 列を `高幡橋（気象庁:111） / ハイドログラフ（水位） / 2025-01-02 / 3日窓` の形式で表示するようにした。
- precheck と batch の両方で日本語表示を共通化した。
- ダブルクリックで出力先を辿る処理は内部 `target_id` ベースのまま維持した。
- `uv run pytest -q tests/hydrology_graphs` で `40 passed`、`uv run pytest -q` で `189 passed` を確認した。

## 完了条件
- 結果一覧の `対象` 列が日本語ラベルになっている。
- ダブルクリックで従来どおり出力先を辿れる。
- precheck と batch で同じ表示ルールを使う。

## 確認方法
- precheck / batch の結果一覧で表示を確認する。
- `target_id` を使う内部処理が壊れていないことを確認する。
- 日本語表示の例が要件・設計と一致することを確認する。

## 関連要件 / 関連設計
- 要件: `docs/dev/requirements/hydrology-graphs-platform.md`
- 設計: `docs/dev/architecture/hydrology_graphs.md`

## 残課題
- なし。
