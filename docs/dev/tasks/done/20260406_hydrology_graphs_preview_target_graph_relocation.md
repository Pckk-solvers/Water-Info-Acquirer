# プレビュー出力対象への対象グラフ移動

## 目的
- `対象グラフ` を `プレビュー出力対象` に移し、プレビューの選択導線を観測所・基準日と同じ枠へまとめる。
- `プレビュー更新` ボタンを一番右へ寄せ、2行目の説明ラベルを廃止する。
- 観測所・基準日を選んだ結果として、`precheck OK` の実在候補だけを候補一覧に出す。

## 対象ファイル
- `src/hydrology_graphs/ui/tabs_style.py`
- `src/hydrology_graphs/ui/execute_actions.py`
- `src/hydrology_graphs/ui/preview_actions.py`
- `src/hydrology_graphs/ui/view_models.py`
- `tests/hydrology_graphs/test_preview_actions.py`
- `tests/hydrology_graphs/test_ui_support.py`

## 実施内容
- `スタイル編集対象` から `対象グラフ` を外す。
- `プレビュー出力対象` に `対象グラフ` を追加する。
- `プレビュー更新` ボタンを最右列へ移動する。
- 2行目の説明ラベルを削除する。
- 観測所・基準日の選択結果に応じて、`precheck OK` の実在候補だけを `対象グラフ` 候補として表示する。
- 不整合な選択が残っていた場合は、実在する候補へ補正する。

## 完了条件
- `対象グラフ` がプレビュー出力対象の枠内にある。
- `プレビュー更新` ボタンが最右にある。
- 2行目の説明ラベルが表示されない。
- 観測所・基準日・対象グラフの組み合わせが実在候補に絞られる。

## 確認方法
- UI 起動後に、プレビュー出力対象の並び順を目視確認する。
- 観測所・基準日を変えて、対象グラフ候補が実在候補だけに絞られることを確認する。
- `uv run pytest -q tests/hydrology_graphs/test_preview_actions.py tests/hydrology_graphs/test_ui_support.py` を実行する。

## 結果
- `対象グラフ` を `プレビュー出力対象` に移動し、`プレビュー更新` ボタンを最右に配置した。
- 2行目の説明ラベルを削除した。
- 観測所・基準日を起点に `precheck OK` の実在候補だけが対象グラフ候補になるようにした。
- 不整合な選択は実在候補へ補正するようにした。
- `uv run pytest -q` を実行し、`196 passed` を確認した。

## 関連要件 / 関連設計
- 要件: `docs/dev/requirements/hydrology-graphs-platform.md`
- 設計: `docs/dev/architecture/hydrology_graphs.md`
