# datetime表記でもイベント終端余白を常時適用

## 目的
- `datetime` 表記時にもイベント系の終端+1時間余白を適用し、表示モードによる窓判定差分をなくす。

## 対象ファイル
- `docs/dev/requirements/hydrology-graphs-platform.md`
- `docs/dev/architecture/hydrology_graphs.md`
- `src/hydrology_graphs/ui/execute_actions.py`
- `src/hydrology_graphs/ui/preview_actions.py`
- `tests/hydrology_graphs/test_preview_actions.py`

## 実施内容
- UI層の `event_window_terminal_padding` 判定を表示モード非依存で常時 `True` にする。
- 要件/設計の「表示モードで切り出し範囲を変える」記述を、常時余白適用の記述へ修正する。
- 既存テストの期待値を更新する。

## 完了条件
- `precheck / preview / batch` の入力において、`datetime` でも `event_window_terminal_padding=True` になる。
- 関連文書の記述が現実装と矛盾しない。

## 確認方法
- `uv run pytest -q tests/hydrology_graphs/test_preview_actions.py tests/hydrology_graphs/test_services.py`

## 関連要件 / 関連設計
- 要件: `docs/dev/requirements/hydrology-graphs-platform.md`
- 設計: `docs/dev/architecture/hydrology_graphs.md`

## 完了結果
- `execute_actions.py` / `preview_actions.py` の `event_window_terminal_padding` 判定を常時 `True` に変更した。
- `datetime` 表記時も、precheck / preview / batch で終端+1時間余白が適用されるようになった。
- 要件/設計文書の表示モードと窓判定に関する記述を現実装に合わせて更新した。
- `uv run pytest -q tests/hydrology_graphs/test_preview_actions.py tests/hydrology_graphs/test_services.py` で通過した。
