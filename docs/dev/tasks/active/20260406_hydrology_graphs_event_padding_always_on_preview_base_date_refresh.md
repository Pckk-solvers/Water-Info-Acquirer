# イベント窓補正の常時有効化と基準日選択の見直し

## 目的
- イベント系の終端+1時間余白を常時有効にし、GUI の切替を廃止する。
- スタイル調整タブの基準日選択を、現状の候補に対して再検証する形へ見直す。
- 既存の precheck / preview / batch の動作を壊さず、イベント系の判定条件を統一する。

## 対象ファイル
- `docs/dev/requirements/hydrology-graphs-platform.md`
- `docs/dev/architecture/hydrology_graphs.md`
- `src/hydrology_graphs/ui/app.py`
- `src/hydrology_graphs/ui/tabs_execute.py`
- `src/hydrology_graphs/ui/execute_actions.py`
- `src/hydrology_graphs/ui/preview_actions.py`
- `src/hydrology_graphs/services/dto.py`
- `src/hydrology_graphs/services/usecases.py`
- `tests/hydrology_graphs/test_ui_support.py`

## 実施内容
- イベント窓補正のチェックボックスを GUI から削除する。
- precheck / preview / batch では常に終端+1時間余白を適用する。
- スタイル調整タブの基準日候補を、選択中値の妥当性を見ながら更新する。
- 無効になった基準日は候補の先頭へ切り替えるか、未選択に戻す。

## 完了条件
- イベント系の終端+1時間余白が常時有効になっている。
- GUI から補正の ON/OFF を切り替える部品が消えている。
- スタイル調整タブの基準日選択が候補の更新に追従する。

## 確認方法
- イベント系の precheck / preview / batch で補正が常時入ることを確認する。
- スタイル調整タブで候補更新後も、基準日選択が無効値のまま残らないことを確認する。

## 関連要件 / 関連設計
- 要件: `docs/dev/requirements/hydrology-graphs-platform.md`
- 設計: `docs/dev/architecture/hydrology_graphs.md`

