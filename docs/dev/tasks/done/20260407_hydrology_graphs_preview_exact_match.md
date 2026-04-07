# プレビュー候補を完全一致で解決する

## 目的
- プレビューの `観測所 / 基準日 / 対象グラフ` 解決を、`precheck OK` の完全一致だけにする。
- 一致しない場合に別候補へフォールバックして誤描画するのをやめる。
- 基準日候補の表示範囲も、選択観測所に応じて狭めてヒット率を上げる。

## 対象ファイル
- `docs/dev/requirements/hydrology-graphs-platform.md`
- `docs/dev/architecture/hydrology_graphs.md`
- `src/hydrology_graphs/ui/execute_actions.py`
- `src/hydrology_graphs/ui/preview_actions.py`
- `src/hydrology_graphs/ui/view_models.py`
- `tests/hydrology_graphs/test_preview_actions.py`
- `tests/hydrology_graphs/test_ui_support.py`

## 実施内容
- プレビュー候補の更新で、選択観測所に応じて基準日候補を狭めるようにした。
- `_resolve_preview_target()` からフォールバックを削除し、完全一致のみ返すようにした。
- 一致しない場合は明示的なエラーメッセージにした。
- 既存テストを新しい一致条件に合わせた。

## 完了結果
- `観測所 / 基準日 / 対象グラフ` が一致する場合のみプレビューが描画されるようになった。
- 一致しない選択でも、別候補に勝手に切り替わらないようになった。
- UI 候補が実在組み合わせに寄るようになった。

## 確認方法
- `uv run pytest -q tests/hydrology_graphs/test_preview_actions.py tests/hydrology_graphs/test_ui_support.py`

## 関連要件 / 関連設計
- 要件: `docs/dev/requirements/hydrology-graphs-platform.md`
- 設計: `docs/dev/architecture/hydrology_graphs.md`
