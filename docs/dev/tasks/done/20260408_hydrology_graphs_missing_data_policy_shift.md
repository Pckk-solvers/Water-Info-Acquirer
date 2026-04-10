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

#### Task 1 実施結果（2026-04-10）

| 層 | 現行動作 | 移行後想定 | 影響範囲 |
|---|---|---|---|
| precheck（services/usecases.py + domain/logic.py） | `validate_event_series_complete` で `value` 欠損または `quality=missing` が1件でもあれば `ng`。理由は `missing_timeseries` として扱う。 | 欠測の種別ごとに `継続可 / 警告付き継続可 / 継続不可` へ分離し、一律 `ng` を廃止。 | `PrecheckItem.status/reason_code` 拡張、`graph_targets_from_precheck_items` の抽出条件見直し、`test_services.py` 更新。 |
| preview（ui/preview_actions.py + services/usecases.py） | プレビュー対象は precheck `ok` のみ。欠測で precheck が落ちるため、欠測系列はプレビュー不能。 | 欠測ありでも継続可の対象はプレビュー可能にし、警告表示を併記。 | `_precheck_ok_targets` 命名/意味の見直し、候補絞り込み条件、表示メッセージ。 |
| batch（ui/execute_actions.py + services/usecases.py） | `ready(ok)` のみ実行対象。欠測系列は precheck で除外されるためバッチ出力対象外。 | 欠測あり継続可を実行対象に含め、結果で「成功（欠測あり）」を識別可能にする。 | 実行対象抽出、結果ステータス語彙、`test_ui_support.py` の状態表示テスト更新。 |
| render（render/plotter.py） | ハイエトは `missing_band` と累積線切断（`where(~missing_mask)`）を実装済み。ハイドロは欠測可視化専用処理なし（NaN区間の線切れはMatplotlib任せ）。 | ハイエト/ハイドロで欠測可視化方針を統一（最低限、線切断と欠測状態の明示）。 | `plotter.py` の共通欠測可視化ロジック追加、`test_plotter.py` 拡張。 |
| UI表示（execute_actions.py / view_models.py） | 結果状態は `ready / precheck_ng` 中心で、欠測あり成功を識別できない。ログも ready/ng 集計のみ。 | 欠測を含む成功状態を表示語彙に追加し、結果一覧・ログで識別可能にする。 | `format_result_status_display`、結果行投入ロジック、サマリ文言、UIテスト更新。 |

#### 差分棚卸しメモ
- 現行では「欠測」原因が `missing_timeseries` に集約され、行欠落 / `quality=missing` / `value` 欠損の区別が外部に出ない。
- レンダリング側はハイエトのみ先行実装で、他グラフとの扱いが不統一。
- precheck と preview/batch の結合が強く、precheck状態語彙の拡張が実質全層に波及する。

### Task 2: 欠測の分類と処理継続条件を定義
- 欠測種別を定義:
  - 行欠落（期待時刻に行なし）
  - `quality=missing`
  - `value` 欠損
- 「描画継続可 / 要警告 / 実行不可」の判定ルールを決める。

#### Task 2 実施結果（2026-04-10）
- 欠測分類を `M1/M2/M3` として確定:
  - `M1`: 行欠落（期待時刻に行なし）
  - `M2`: `quality=missing`
  - `M3`: `value` 欠損（`null/NaN`）
- 判定状態を `ok / warn / ng` の3段階で確定:
  - `ok`: 欠測なし、または欠測があっても条件上問題なし
  - `warn`: 欠測ありだが継続可（可視化・警告必須）
  - `ng`: 継続不可
- 継続可の最小条件を確定:
  - 時刻列が解釈可能
  - 対象窓に1件以上の有効データ（`value` 数値かつ `quality!=missing`）が存在
- 反映先:
  - 要件: `docs/dev/requirements/hydrology-graphs-platform.md` 10.5
  - 設計: `docs/dev/architecture/hydrology_graphs.md`（services/render/UI の責務追記）

### Task 3: precheck判定ロジックの移行
- 欠測を理由に一律 `ng` にしない設計へ変更する。
- 判定結果に「欠測あり（描画継続）」の状態を追加する。

#### Task 3 実施結果（2026-04-10）
- `domain/logic.py` に `evaluate_event_series_status` を追加し、イベント窓を `ok / warn / ng` で判定するよう変更。
- `services/usecases.py` で precheck 判定に `warn` を導入。
  - 欠測ありでも有効データが残る場合は `warn`（`reason_code=missing_with_warning`）へ移行。
  - 有効データが存在しない場合のみ `ng` 維持。
- `PrecheckSummary` に `warn_targets` を追加。
- UI表示の前提として `precheck_warn`（欠測あり継続可）語彙を追加。
- 描画呼び出し側は `warn` を継続可能として受理するように更新（Task 4 で層間統一を継続）。

### Task 4: preview/batch/renderの挙動統一
- preview と batch で同じ欠測処理規約を使う。
- render では欠測区間の可視化（欠測帯、線切断）を統一する。
- 層ごとの挙動差が残らないように共通化ポイントを定める。

#### Task 4 実施結果（2026-04-10）
- preview / batch / render の欠測判定入口を `services/usecases._evaluate_target`（`ok/warn/ng`）へ統一。
  - `_render_target_bytes` は `warn` を描画継続対象として受理。
  - precheckから preview/batch へ渡す対象抽出は `ok/warn` を採用。
- render の欠測可視化をイベント系で統一。
  - ハイエト: 既存の欠測帯 + 累積線切断を維持。
  - ハイドロ: 1時間リインデックスで欠測区間を `NaN` 化して線切断し、`missing_band.enabled=true` 時は欠測帯を描画。
- 欠測表示既定値を補強。
  - ハイドロ系スタイルに `missing_band` 既定を追加（enabled=true, color, alpha）。
  - 正規化で `missing_band` 未指定時の既定値を補完。

### Task 5: UI/表示メッセージ整備
- 欠測を含む対象であることを、プレビュー・結果一覧・ログで明示する。
- ユーザーが「描画失敗」と「欠測を含む描画成功」を識別できる表示にする。

#### Task 5 実施結果（2026-04-10）
- 結果一覧の状態語彙を拡張:
  - `precheck_warn` / `warn` を「欠測あり（継続可）」として表示。
- precheck サマリ表示とログを拡張:
  - `READY / WARN / NG` を同時表示。
- プレビュー表示を拡張:
  - `warn` で成功した場合、プレビュー更新メッセージに欠測理由を併記。
- バッチ結果を拡張:
  - `warn` で成功した対象は `status=success` のまま `reason_code=missing_with_warning` と `reason_message` を保持。

### Task 6: テスト追加（部分テスト）
- 欠測を含む正常ケース（描画継続）を追加する。
- 欠測で継続不可なケースの境界条件を追加する。
- 実行コマンド（部分テスト）:
  - `uv run pytest -q tests/hydrology_graphs/test_services.py tests/hydrology_graphs/test_plotter.py tests/hydrology_graphs/test_ui_support.py`

#### Task 6 実施結果（2026-04-10）
- 追加/更新テスト:
  - `test_precheck_event_padding_requires_terminal_hour`（`ng`→`warn` へ期待更新）
  - `test_preview_graph_target_returns_warning_on_missing_but_renderable`
  - `test_run_graph_batch_honors_event_padding`（`success + missing_with_warning`）
  - `test_render_hydro_breaks_line_and_draws_missing_band_on_missing`
  - `test_default_hydro_styles_enable_missing_band_defaults`
  - `test_format_result_status_display_uses_japanese_labels`（warn語彙）
  - `test_handle_preview_done_success_with_warning_message`
- 実行結果:
  - `uv run pytest -q tests/hydrology_graphs/test_services.py tests/hydrology_graphs/test_ui_support.py tests/hydrology_graphs/test_preview_actions.py tests/hydrology_graphs/test_event_handlers.py tests/hydrology_graphs/test_plotter.py tests/hydrology_graphs/test_style_store.py`
  - `78 passed`

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

### 実装着手前レビュー結果（2026-04-10）
- 観点: 欠測の分類がデータ契約と矛盾していないか  
  - 判定: OK（契約上の `value` / `quality` / 時刻欠落で分類可能）
- 観点: 層間で判定基準が統一されているか  
  - 判定: NG（precheck厳格判定に対してrender側は一部可視化済みで不一致）
- 観点: 回帰テスト計画が十分か  
  - 判定: 要補強（status語彙拡張時のUI表示・抽出条件テストを追加する必要あり）

## 完了時の追加整合チェック
- タスク完了時に、`requirements / architecture / adr / reference(style-contract)` の記述差分を再点検し、欠測方針の語彙・判定条件をそろえる。
- 完了移動（`done`）前に、方針矛盾（旧「欠測NG」記述の残存）がないことを確認する。
