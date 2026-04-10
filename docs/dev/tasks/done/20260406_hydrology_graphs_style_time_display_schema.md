# スタイル JSON への表示モード正本化

## 目的
- スタイル JSON に `display.time_display_mode` を追加し、表示モードの保存先を正本化する。
- 既存のスタイル JSON を読み込んだときは `datetime` を既定値として補完し、後方互換を維持する。

## 対象ファイル
- `src/hydrology_graphs/io/style_store.py`
- `tests/hydrology_graphs/test_style_store.py`

## 実施内容
- `default_style()` に `display.time_display_mode` の既定値を含めた。
- `load_style()` / `save_style()` で `display.time_display_mode` を正規化した。
- 既存 JSON に `display` が無い場合でも `datetime` に補完されるようにした。
- 保存時に `display.time_display_mode` が欠落しないことを確認するテストを追加した。

## 完了条件
- 新規生成される style JSON に `display.time_display_mode` が入る。
- 既存 style JSON を読み込むと `datetime` が補完される。
- 不正な `display` 値でも、保存/読込の挙動が壊れない。

## 確認方法
- `uv run pytest -q tests/hydrology_graphs/test_style_store.py` を実行し、通過を確認した。

## 関連要件 / 関連設計
- 要件: `docs/dev/requirements/hydrology-graphs-platform.md`
- 設計: `docs/dev/architecture/hydrology_graphs.md`

## 結果
- `display.time_display_mode` を style JSON のルートに追加した。
- `datetime` 既定と invalid 値の補完を実装した。
- `uv run pytest -q tests/hydrology_graphs/test_style_store.py` で通過を確認した。

## 残課題
- なし。
