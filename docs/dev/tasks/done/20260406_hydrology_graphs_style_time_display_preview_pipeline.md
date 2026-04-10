# プレビューとサンプル出力への表示モード伝播

## 目的
- preview と developer mode の sample output が style JSON 上の表示モードを参照するようにする。
- 画面上の選択値ではなく、正規化済みの style payload を通して表示モードを渡す。

## 対象ファイル
- `src/hydrology_graphs/ui/preview_actions.py`
- `src/hydrology_graphs/services/usecases.py`
- `tests/hydrology_graphs/test_services.py`

## 実施内容
- preview / sample output の入力生成時に表示モードを読み取るようにした。
- 表示モードを preview usecase に伝播させた。
- developer mode の sample output でも同じ表示モードを使うようにした。
- 既存の preview 実行フローを壊さないようにした。

## 完了条件
- preview と sample output が同じ表示モード入力を使う。
- style JSON の値が preview 実行に反映される。
- 既存のプレビュー生成テストが通る。

## 確認方法
- `uv run pytest -q tests/hydrology_graphs/test_preview_actions.py tests/hydrology_graphs/test_services.py` を実行し、通過を確認した。

## 関連要件 / 関連設計
- 要件: `docs/dev/requirements/hydrology-graphs-platform.md`
- 設計: `docs/dev/architecture/hydrology_graphs.md`

## 結果
- `build_preview_input()` が `display.time_display_mode` を参照するようにした。
- `PreviewInput` へ payload ベースの表示モードを渡すようにした。
- `uv run pytest -q tests/hydrology_graphs/test_preview_actions.py tests/hydrology_graphs/test_services.py` を含む関連テストで通過を確認した。

## 残課題
- なし。
