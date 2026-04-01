# 時刻契約統一: 実装タスク分解

Status: target
Updated: 2026-04-01
Related:
- `datetime-internal-unification-requirements.md` (Why/What)
- `datetime-internal-unification-impact-analysis.md` (Where)

## 1. 目的と完了条件
- 目的:
  - `datetime` 内部統一を、実装者が迷わず着手できる粒度で分解する。
  - `display_dt` 廃止、`period_*` 契約、`request_failed` 運用を実装順で固定する。
- 完了条件:
  - T1〜T10 がすべて完了し、受け入れ基準（AC-01〜AC-08）を満たす。

## 2. 決定事項（固定ルール）
- `1day.period_end_at = 翌日 00:00:00`
- `request_failed` は実行ログのみ（レコード化しない）
- `quality` は `normal` / `missing` のみ
- `display_dt` は廃止（内部・保存・中間で保持しない）
- イベント窓判定とグラフ時刻軸は `period_end_at` 基準

## 3. タスク一覧（T1〜T10）

### T1 契約固定（domain contract）
- 対象: `river_meta.rainfall` ドメイン契約
- 変更:
  - `period_start_at` / `period_end_at` を正式契約に追加
  - `observed_at` は `period_end_at` 同義で段階互換
- Done:
  - 型定義・契約文言・利用側前提が一致
- 必須テスト:
  - 契約シリアライズ/デシリアライズ互換

### T2 正規化共通化（normalizer）
- 対象: source共通正規化
- 変更:
  - `normalize_period(...)` を導入
  - `24:00` / `23:59:59.999999` を翌日00:00へ統一
- Done:
  - source別 `-1h` 補正が主経路から消える
- 必須テスト:
  - 時刻正規化の境界ケース（hour=24、疑似24時）

### T3 取得境界統一（request window）
- 対象: request計画と最終フィルタ
- 変更:
  - 取得窓拡張と出力対象フィルタを分離
  - 共通フィルタ: `period_start_at >= user_start_at` かつ `period_end_at <= user_end_at`
- Done:
  - 余分取得が最終出力に混入しない
- 必須テスト:
  - 年跨ぎ（2025年指定で2026年1月不要データが除外）

### T4 Parquet契約移行
- 対象: parquet write/read
- 変更:
  - `period_*` 保存対応
  - 旧フォーマット読込互換
  - `display_dt - 1h` フォールバック削除
- Done:
  - 新旧Parquetの読込成功 + 新契約保存成功
- 必須テスト:
  - 旧Parquet読込→新Parquet再保存

### T5 CSV/Excel出力置換
- 対象: CSV/Excel出力
- 変更:
  - `period_end_at` から表示列を生成
  - 独自24時ロジックを排除し共通変換のみ使用
- Done:
  - 出力件数・時刻境界が契約と一致
- 必須テスト:
  - 24時相当行の表示・件数一致

### T6 Graph/Analysis置換
- 対象: 分析・グラフ描画
- 変更:
  - 欠測判定、再index、窓判定を `period_end_at` 基準へ統一
- Done:
  - Excelとグラフの時刻整合が一致
- 必須テスト:
  - 同一入力でExcel/Graphの先頭末尾時刻一致

### T7 `display_dt` 参照除去
- 対象: `water_info` 本流 + tests
- 変更:
  - `display_dt` 依存コード/テストを新契約へ置換
- Done:
  - `rg -n "display_dt" src/water_info tests/water_info` が移行注記を除き0件
- 必須テスト:
  - 旧 `display_dt` 前提テストの置換完了

### T8 失敗分類統一
- 対象: 実行結果・ログ
- 変更:
  - `missing` と `request_failed` を明確分離
  - `request_failed` はログのみ記録
  - 実行ログ項目（`run_id`, `source`, `station_key`, `interval`, `user_start_at`, `user_end_at`, `error_code`, `error_message`, `retry_count`, `occurred_at`）を固定
- Done:
  - レコード中に `request_failed` が混入しない
- 必須テスト:
  - 完全失敗・部分欠落の分離検証

### T9 回帰テスト再定義
- 対象: `tests/water_info`, `tests/river_meta`, `tests/hydrology_graphs`
- 変更:
  - 契約更新後の境界テストへ差し替え
- Done:
  - 契約ベースの最小回帰網が成立
- 必須テスト:
  - 月初00:00、24時相当、年跨ぎ、欠測/失敗分離

### T10 旧ロジック撤去と完了宣言
- 対象: 旧補正ロジックと運用文書
- 変更:
  - `drop_last_each` 依存、個別 `-1h`、`display_dt` 逆算保存を撤去
  - target→archive移行条件を満たす
- Done:
  - 旧補正が主経路に残らない
- 必須テスト:
  - 変更前後差分レポート（件数・先頭末尾時刻）

## 4. 依存関係（実装DAG）
- T1 → T2 → T3 → T4 → T5/T6 → T7 → T8 → T9 → T10
- 並列可:
  - T5 と T6
  - T8 は T4 完了後に先行可能

## 5. 受け入れ基準（AC）
- AC-01: `period_*` を使った内部契約が全対象で統一される
- AC-02: 24時相当は翌日00:00で一貫する
- AC-03: 年跨ぎの余分取得が最終出力に残らない
- AC-04: `display_dt` は本流コード/テストから除去される
- AC-05: `request_failed` はログのみで管理される
- AC-06: 旧Parquet互換読込 + 新契約保存が可能
- AC-07: Excel/Graphの時刻境界と件数が一致
- AC-08: 主要回帰テストが更新後に成功

## 6. スプリント実行順（推奨）
- Sprint 1: T1, T2, T3
- Sprint 2: T4, T5, T6
- Sprint 3: T7, T8, T9, T10

## 7. リスクとロールバック
- リスク:
  - 既存Parquet互換崩れ
  - 出力件数差分による運用混乱
- 対策:
  - 旧読込互換を先に実装し、再保存で新契約へ寄せる
  - 変更前後で件数・先頭末尾時刻の差分レポートを必須化

## 8. 実行コマンド（固定）
- スプリント1終了時:
  - `uv run pytest tests/river_meta -k "normalizer or period"`
  - `uv run pytest tests/jma_rainfall_pipeline -k "hourly_table_parser"`
- スプリント2終了時:
  - `uv run pytest tests/jma_rainfall_pipeline -k "parquet_exporter"`
  - `uv run pytest tests/water_info -k "dataframe or flow or parquet"`
  - `uv run pytest tests/hydrology_graphs -k "parquet_store or domain"`
- スプリント3終了時:
  - `uv run pytest tests/river_meta tests/jma_rainfall_pipeline tests/water_info tests/hydrology_graphs`
