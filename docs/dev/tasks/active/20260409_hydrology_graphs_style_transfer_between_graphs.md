# グラフ間スタイル適用機能（後続タスク）

## 目的
- あるグラフで細かく調整したスタイルを、別グラフへ再利用できるようにする。
- 実装共通化ではなく、ユーザー操作として「設定の転用」を提供する。

## 対象ファイル
- `docs/dev/requirements/hydrology-graphs-platform.md`
- `docs/dev/architecture/hydrology_graphs.md`
- `docs/dev/reference/hydrology-graphs-platform/style-contract.md`
- `src/hydrology_graphs/ui/app.py`
- `src/hydrology_graphs/ui/tabs_style.py`
- `src/hydrology_graphs/io/style_store.py`
- `tests/hydrology_graphs/test_ui_support.py`
- `tests/hydrology_graphs/test_style_store.py`

## 実施内容
1. グラフ間コピー（単体）
- 適用元グラフを選び、現在対象グラフへコピーするUIを追加する。
- コピー方式:
  - 全項目コピー
  - 共通項目のみコピー（存在キーのみ）

2. 一括適用（複数対象）
- 対象グラフを複数選択して同時適用できるようにする。
- 適用後に件数（適用/スキップ）を表示する。

3. プリセット保存/適用
- 現在スタイルを名前付きで保存し、任意グラフへ適用できるようにする。
- 非対応キーはスキップする（エラー停止しない）。

4. 安全策
- すべて履歴（undo/redo）対象にする。
- 適用時は部分更新を維持し、不要な全体再構築を行わない。

## 完了条件
- 1グラフで調整したスタイルを別グラフへ適用できる。
- 一括適用とプリセット適用が動作する。
- 非対応キーは安全にスキップされる。
- 回帰テストが追加され、部分検証が通る。

## 確認方法
- 手動:
  - ハイエトで変更した系列/棒設定をハイドロへ適用し、反映を確認
  - 複数グラフ同時適用の結果件数を確認
  - プリセット保存→再適用→反映確認
- コマンド:
  - `uv run ruff check src/hydrology_graphs/ui src/hydrology_graphs/io tests/hydrology_graphs/test_ui_support.py tests/hydrology_graphs/test_style_store.py`
  - `uv run pyright src/hydrology_graphs/ui src/hydrology_graphs/io`
  - `uv run pytest -q tests/hydrology_graphs/test_ui_support.py tests/hydrology_graphs/test_style_store.py`

## 関連要件/関連設計
- `docs/dev/reference/hydrology-graphs-platform/style-contract.md`
- `docs/dev/tasks/active/20260409_hydrology_graphs_style_setting_candidates_proposal.md`

## 実装着手前の自己レビュー結果（観点と判定）
- 観点1: 既存JSON互換性を壊さないか
  - 判定: OK（非対応キーはスキップ方針）
- 観点2: UI複雑化が過剰でないか
  - 判定: OK（段階実装: 単体コピー→一括適用→プリセット）
- 観点3: 既存の部分更新方針と矛盾しないか
  - 判定: OK（適用後は差分更新を前提にする）
