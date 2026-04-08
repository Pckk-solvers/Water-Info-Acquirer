# hydrology_graphs UI リファクタリング計画（`ui/app.py` 分割）

## 目的
- 肥大化した `src/hydrology_graphs/ui/app.py` の責務を分割し、変更容易性と不具合調査性を改善する。
- 既存挙動を変えずに段階的に分離する。

## 対象ファイル
- `docs/dev/requirements/hydrology-graphs-platform.md`
- `docs/dev/architecture/hydrology_graphs.md`
- `src/hydrology_graphs/ui/app.py`
- `src/hydrology_graphs/ui/`（新規モジュール追加）
- `tests/hydrology_graphs/test_ui_support.py`
- `tests/hydrology_graphs/test_preview_actions.py`

## 実施内容（1回で実装・確認できる粒度）

### Task 1: 分割境界の固定（ドキュメント）
- `app.py` の責務を以下に分ける方針を設計文書へ明記する。
  - coordinator（状態保持と委譲）
  - style form builder（フォーム構築）
  - style form actions（フォーム値反映）
  - style palette dialog（設定ダイアログ）
  - station/base date selection（実行タブの選択UI）
- 完了条件:
  - architecture / requirements に分割方針と制約が反映されている。

### Task 2: style form builder の切り出し
- `app.py` からフォーム構築系を `ui/style_form_builder.py` へ移す。
- `app.py` 側には薄いラッパを残し、既存呼び出しの互換を維持する。
- 完了条件:
  - フォーム表示挙動に差分がない。
  - 部分テストが通る。

#### Task 2 実施結果
- 実施日: 2026-04-08
- 追加:
  - `src/hydrology_graphs/ui/style_form_builder.py`
- 変更:
  - `src/hydrology_graphs/ui/app.py` の以下を builder へ委譲化
    - `_create_style_control`
    - `_create_compact_style_row`
    - `_create_compact_input_control`
    - `_create_palette_style_row`
    - `_build_palette_summary`
    - `_style_label_column_minsize`
- 確認:
  - `uv run ruff check ...` は通過
  - `uv run pyright ...` は既存の `app.py` 属性注釈不足に起因するエラー群が残る（今回分離で新規悪化なし）
  - `uv run pytest -q tests/hydrology_graphs/test_ui_support.py tests/hydrology_graphs/test_preview_actions.py` は通過

### Task 3: style form actions の切り出し
- 値反映と型変換、group toggle 制御を `ui/style_form_actions.py` へ移す。
- 完了条件:
  - `反映/Enter/JSON同期` の動作が維持される。
  - 部分テストが通る。

#### Task 3 実施結果
- 実施日: 2026-04-08
- 追加:
  - `src/hydrology_graphs/ui/style_form_actions.py`
- 変更:
  - `src/hydrology_graphs/ui/app.py` の以下を actions へ委譲化
    - `_set_control_var`
    - `_apply_group_toggle_states`
    - `_apply_style_form_values`
    - `_coerce_control_value`
- 確認:
  - `uv run ruff check ...` は通過
  - `uv run python -m py_compile ...` は通過
  - `uv run pytest -q tests/hydrology_graphs/test_ui_support.py tests/hydrology_graphs/test_preview_actions.py` は通過
  - `uv run pyright src/hydrology_graphs/ui/app.py src/hydrology_graphs/ui/style_form_builder.py src/hydrology_graphs/ui/style_form_actions.py` は通過（0 errors）

### Task 4: palette dialog の切り出し
- カラーパレット/設定ダイアログ処理を `ui/style_palette_dialog.py` へ分離する。
- 完了条件:
  - 設定ダイアログの表示・適用・再適用が維持される。
  - 部分テストが通る。

#### Task 4 実施結果
- 実施日: 2026-04-08
- 追加:
  - `src/hydrology_graphs/ui/style_palette_dialog.py`
- 変更:
  - `src/hydrology_graphs/ui/app.py` の以下を dialog へ委譲化
    - `_open_palette_dialog`
    - `_is_hex_color`
- 確認:
  - `uv run ruff check ...` は通過
  - `uv run pyright ...` は通過（0 errors）
  - `uv run pytest -q tests/hydrology_graphs/test_ui_support.py tests/hydrology_graphs/test_preview_actions.py` は通過

### Task 5: 実行タブ選択UIの切り出し
- 観測所チェックUIと基準日UIを `ui/station_selection.py` / `ui/base_date_selection.py` へ分離する。
- 完了条件:
  - 観測所選択、基準日候補反映、CSV入出力の挙動が維持される。
  - 部分テストが通る。

#### Task 5 実施結果
- 実施日: 2026-04-08
- 追加:
  - `src/hydrology_graphs/ui/station_selection.py`
  - `src/hydrology_graphs/ui/base_date_selection.py`
- 変更:
  - `src/hydrology_graphs/ui/app.py` の実行タブ選択UI系を委譲化
    - 観測所選択: 描画/トグル/クリック判定/全選択/全解除/チェック反映
    - 基準日候補: 候補再計算/年月日候補更新/候補ISO解決
    - 詳細読込確保: `_ensure_full_catalog_loaded`
- 確認:
  - `uv run ruff check ...` は通過
  - `uv run pyright ...` は通過（0 errors）
  - `uv run pytest -q tests/hydrology_graphs/test_ui_support.py tests/hydrology_graphs/test_preview_actions.py` は通過

### Task 6: 後処理（ラッパ削減）
- 各切り出し後に不要なラッパを削減し、`app.py` を coordinator 中心へ整理する。
- 完了条件:
  - `app.py` が「画面状態 + 委譲」に集中している。
  - 挙動差分がない。

#### Task 6 実施結果
- 実施日: 2026-04-08
- 変更:
  - `src/hydrology_graphs/ui/app.py` から以下の不要ラッパを削除
    - `_create_style_control`
    - `_create_compact_style_row`
    - `_create_compact_input_control`
    - `_create_palette_style_row`
    - `_build_palette_summary`
    - `_style_label_column_minsize`
  - `app.py` 内の呼び出しは `style_form_builder` 関数の直接呼び出しへ統一
  - 未使用 import（`create_compact_input_control`）を削除
- 確認:
  - `uv run ruff check src/hydrology_graphs/ui/app.py src/hydrology_graphs/ui/style_form_builder.py docs/dev/tasks/active/20260408_hydrology_graphs_ui_app_refactoring_plan.md` は通過
  - `uv run pyright src/hydrology_graphs/ui/app.py src/hydrology_graphs/ui/style_form_builder.py` は通過（0 errors）
  - `uv run pytest -q tests/hydrology_graphs/test_ui_support.py tests/hydrology_graphs/test_preview_actions.py` は通過（16 passed）

## 完了条件
- 分割後もユーザー向け挙動（UIレイアウト、反映、プレビュー、実行）が変わらない。
- `app.py` の責務が coordinator 中心に縮小されている。
- 少なくとも以下の部分テストが通る:
  - `uv run pytest -q tests/hydrology_graphs/test_ui_support.py tests/hydrology_graphs/test_preview_actions.py`

## 確認方法
- スタイル調整タブ:
  - フォーム表示
  - 反映
  - JSON同期
  - プレビュー更新
- 条件設定・実行タブ:
  - 観測所チェック
  - 基準日候補更新
  - CSV入出力
- 上記テストを実行して回帰がないことを確認する。

## 関連要件 / 関連設計
- 要件: `docs/dev/requirements/hydrology-graphs-platform.md`
- 設計: `docs/dev/architecture/hydrology_graphs.md`

## 実装着手前レビュー
- この計画は Task 1 の反映後、実装着手前にレビュー承認を得る。
- 承認観点:
  - 分割境界が責務と一致しているか
  - 既存テストで回帰を検出できるか
  - 1タスクあたりの差分が小さいか

## 実装着手前の自己レビュー結果（Task 2）
- 観点1: 分割境界
  - 判定: OK
  - 理由: フォーム構築（widget生成/配置）のみを `style_form_builder` に切り出し、値反映ロジックは `app.py` 側に残すため責務が混ざらない。
- 観点2: 互換性
  - 判定: OK
  - 理由: `app.py` 側に同名メソッドの薄いラッパを残して既存呼び出し互換を維持する。
- 観点3: テスト範囲
  - 判定: OK
  - 理由: `test_ui_support.py` と `test_preview_actions.py` を最低ラインとして回帰確認可能。

## 実装着手前の自己レビュー結果（Task 3）
- 観点1: 分割境界
  - 判定: OK
  - 理由: 値反映・型変換・group toggle のみを `style_form_actions` に分離し、UIレイアウト構築は builder 側に残す。
- 観点2: 互換性
  - 判定: OK
  - 理由: `app.py` 側メソッド名は維持し、内部で actions 関数へ委譲するため既存呼び出しを崩さない。
- 観点3: テスト範囲
  - 判定: OK
  - 理由: 既存の UI 部分テストでフォーム反映・プレビューアクション回帰を確認できる。

## 実装着手前の自己レビュー結果（Task 4）
- 観点1: 分割境界
  - 判定: OK
  - 理由: ダイアログ表示・入力検証・適用処理は `style_palette_dialog` に閉じており、builder/actions とは責務が分離できる。
- 観点2: 互換性
  - 判定: OK
  - 理由: `app.py` 側に `_open_palette_dialog` と `_is_hex_color` のラッパを維持し、既存ボタンコールバック互換を保つ。
- 観点3: テスト範囲
  - 判定: OK
  - 理由: `test_ui_support.py` と `test_preview_actions.py` の回帰確認に加え、`ruff/pyright` で静的検証を行う。

## 実装着手前の自己レビュー結果（Task 5）
- 観点1: 分割境界
  - 判定: OK
  - 理由: 観測所選択UIと基準日候補UIは実行タブ内の表示/選択責務でまとまっており、`app` 本体から切り離せる。
- 観点2: 互換性
  - 判定: OK
  - 理由: `app.py` 側に同名ラッパを残し、イベントバインド先や既存呼び出しを変更しない。
- 観点3: テスト範囲
  - 判定: OK
  - 理由: `test_ui_support.py` / `test_preview_actions.py` で選択UIとプレビュー候補回帰を確認できる。

## 実装着手前の自己レビュー結果（Task 6）
- 観点1: 分割後の責務
  - 判定: OK
  - 理由: wrapper 削減は「委譲済み関数の直接呼び出し」へ寄せるのみで、責務境界を壊さない。
- 観点2: 互換性リスク
  - 判定: OK
  - 理由: 外部参照されるコールバック名（バインド先）は維持し、内部専用の薄いラッパのみ削減する。
- 観点3: 検証可能性
  - 判定: OK
  - 理由: `ruff / pyright / test_ui_support / test_preview_actions` で回帰確認が可能。
