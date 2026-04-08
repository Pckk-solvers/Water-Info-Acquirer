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

### Task 3: style form actions の切り出し
- 値反映と型変換、group toggle 制御を `ui/style_form_actions.py` へ移す。
- 完了条件:
  - `反映/Enter/JSON同期` の動作が維持される。
  - 部分テストが通る。

### Task 4: palette dialog の切り出し
- カラーパレット/設定ダイアログ処理を `ui/style_palette_dialog.py` へ分離する。
- 完了条件:
  - 設定ダイアログの表示・適用・再適用が維持される。
  - 部分テストが通る。

### Task 5: 実行タブ選択UIの切り出し
- 観測所チェックUIと基準日UIを `ui/station_selection.py` / `ui/base_date_selection.py` へ分離する。
- 完了条件:
  - 観測所選択、基準日候補反映、CSV入出力の挙動が維持される。
  - 部分テストが通る。

### Task 6: 後処理（ラッパ削減）
- 各切り出し後に不要なラッパを削減し、`app.py` を coordinator 中心へ整理する。
- 完了条件:
  - `app.py` が「画面状態 + 委譲」に集中している。
  - 挙動差分がない。

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
