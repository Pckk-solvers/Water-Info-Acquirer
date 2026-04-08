# 欠測データ方針を「排除」から「可視化・継続処理」へ移行する

## 目的
- 欠測を含む時系列を一律で除外する現行方針から、欠測を保持したまま描画・確認できる方針へ移行する。
- `precheck / preview / batch / render / UI` の欠測扱いを整合させる。

## 対象ファイル
- `docs/dev/requirements/hydrology-graphs-platform.md`
- `docs/dev/architecture/hydrology_graphs.md`
- `docs/dev/reference/hydrology-graphs-platform/parquet-contract.md`
- `src/hydrology_graphs/services/usecases.py`
- `src/hydrology_graphs/domain/logic.py`
- `src/hydrology_graphs/render/plotter.py`
- `src/hydrology_graphs/ui/execute_actions.py`
- `src/hydrology_graphs/ui/preview_actions.py`
- `tests/hydrology_graphs/test_services.py`
- `tests/hydrology_graphs/test_plotter.py`
- `tests/hydrology_graphs/test_ui_support.py`

## 実施内容（実装前提の分解）

### Task 1: 現行実装との差分棚卸し
- 欠測で NG 判定している箇所を層ごとに洗い出す。
- 対象: `precheck`, `preview`, `batch`, `render`, `UI表示`。
- 出力: 「現行動作 / 移行後想定 / 影響範囲」の差分表を作成する。

### Task 2: 欠測の分類と処理継続条件を定義
- 欠測種別を定義:
  - 行欠落（期待時刻に行なし）
  - `quality=missing`
  - `value` 欠損
- 「描画継続可 / 要警告 / 実行不可」の判定ルールを決める。

### Task 3: precheck判定ロジックの移行
- 欠測を理由に一律 `ng` にしない設計へ変更する。
- 判定結果に「欠測あり（描画継続）」の状態を追加する。

### Task 4: preview/batch/renderの挙動統一
- preview と batch で同じ欠測処理規約を使う。
- render では欠測区間の可視化（欠測帯、線切断）を統一する。
- 層ごとの挙動差が残らないように共通化ポイントを定める。

### Task 5: UI/表示メッセージ整備
- 欠測を含む対象であることを、プレビュー・結果一覧・ログで明示する。
- ユーザーが「描画失敗」と「欠測を含む描画成功」を識別できる表示にする。

### Task 6: テスト追加（部分テスト）
- 欠測を含む正常ケース（描画継続）を追加する。
- 欠測で継続不可なケースの境界条件を追加する。
- 実行コマンド（部分テスト）:
  - `uv run pytest -q tests/hydrology_graphs/test_services.py tests/hydrology_graphs/test_plotter.py tests/hydrology_graphs/test_ui_support.py`

## 完了条件
- 欠測を含む時系列が、規定条件下で precheck/preview/batch を通過できる。
- 欠測区間の可視化と警告表示が仕様どおり実装される。
- 要件文書と実装の差分記録が残っている。

## 確認方法
- 欠測を意図的に含むテストデータで precheck/preview/batch を実行し、期待状態になることを確認する。
- 結果一覧とログに欠測状態が正しく表示されることを確認する。
- 指定の部分テストが通過することを確認する。

## 関連要件 / 関連設計
- 要件: `docs/dev/requirements/hydrology-graphs-platform.md`（10章）
- 設計: `docs/dev/architecture/hydrology_graphs.md`

## 実装着手前レビュー
- 本タスクは影響範囲が広いため、Task 1 の差分棚卸しレビュー承認後に実装へ進む。
- レビュー観点:
  - 欠測の分類がデータ契約と矛盾していないか
  - 層間で判定基準が統一されているか
  - 回帰テスト計画が十分か
