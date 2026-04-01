# 時刻契約統一: 影響範囲調査と修正導線（詳細）

Status: target
Updated: 2026-04-01
Related:
- `datetime-internal-unification-requirements.md`
- `datetime-internal-unification-task-breakdown.md`

Role:
- requirements = Why/What
- impact-analysis = Where
- task-breakdown = How/When

## 1. この文書の目的
- 現実装のどこに時刻ロジックが分散しているかを特定する。
- どの順番で直すべきか（関連路軸）を定義する。
- 不確定事項を `[要確認]` として残し、実装判断の論点を明確化する。

## 2. 前提（今回の統一方針）
- 内部表現はすべて `datetime`。
- 1時間値は `period_start_at` / `period_end_at` を持つ区間値として扱う。
- 24時は `period_end_at = 翌日00:00:00`。
- `missing` と `request_failed` は分離する。

## 3. 関連路軸（修正順序）
1. 契約と型を先に固定（domain/model）
2. 取得・パースを契約へ合わせる（request/parse）
3. 永続化契約を更新（parquet write/read）
4. 集計・解析を新契約へ合わせる（analysis/service）
5. グラフ・Excel表示を新契約へ合わせる（render/output）
6. UI・実行ログ・エラー表示を更新
7. 旧互換の縮退と削除

理由:
- 出力側から先に直すと、入力/保存の揺れで再崩壊するため。

## 4. 影響範囲マップ（ファイル単位）

### 4.1 water_info 系

#### A-01 [高] `src/water_info/service/flow_fetch.py`
- 現状:
  - URL月列を生成し、`drop_last_each=mode_type in ["S","U"]` で末尾切り捨てを実施。
  - `start_date = datetime(..., 0, 0)` から連番で時刻を再構築。
- 問題:
  - 末尾調整がデータ契約でなくスクレイピング都合に依存。
  - 区間情報（`period_*`）を持たない。
- 修正:
  - `drop_last_each` ベースの補正を廃止。
  - 取得結果を時刻区間へ正規化する共通関数へ移譲。
  - 取得窓拡張の計画（前日/前月）と最終境界フィルタを明示化。
- 副作用:
  - 既存件数と先頭/末尾時刻が変わる可能性。
- テスト:
  - 月初00:00、月末24時相当、年跨ぎの件数一致。

#### A-02 [高] `src/water_info/infra/dataframe_utils.py`
- 現状:
  - `display_dt = datetime + 1h` を内包。
- 問題:
  - 表示都合の列が中間データに混在。
- 修正:
  - 中間表現は `datetime` + `period_*` のみに寄せる。
  - `display_dt` は出力層で派生生成に限定。
- 副作用:
  - 既存Excel生成ロジックの入力列が変わる。

#### A-03 [高] `src/water_info/entry.py`
- 現状:
  - Parquet保存時に `datetime` 優先、無ければ `display_dt - 1h` で `observed_at` を生成。
  - コメントで Hydro時刻運用を前提化。
- 問題:
  - 逆算保存が契約違反リスク。
  - `period_*` 未保存。
- 修正:
  - 保存スキーマへ `period_start_at`/`period_end_at` 追加。
  - `display_dt` 逆算廃止。
  - `request_failed` は通常レコードとして保存しない。
- 副作用:
  - 既存Parquet読込互換が必要。

#### A-04 [中] `src/water_info/service/flow_write.py` / `src/water_info/infra/excel_summary.py`
- 現状:
  - `display_dt` を直接利用してExcel構築。
- 問題:
  - 内部契約と表示契約が密結合。
- 修正:
  - Excel直前に `period_end_at` から表示列を生成。
- 副作用:
  - 既存テンプレートの列位置再調整。

#### A-05 [中] `src/water_info/postprocess.py`
- 現状:
  - `display_dt - 1h` から `hydro_date` を作成。
- 問題:
  - 新契約では不要または置換対象。
- 修正:
  - `period_start_at` 基準へ置換。
- 決定:
  - 本モジュールは主経路から外し、今回の時刻契約統一の必須改修対象に含めない（保守モード）。

### 4.2 jma_rainfall_pipeline 系

#### B-01 [高] `src/jma_rainfall_pipeline/parser/hourly_table_parser.py`
- 現状:
  - `hour == 24` を翌日00:00へ変換。
- 評価:
  - 方向性は契約に一致。共通正規化へ接続対象。
- 修正:
  - parser出力を `period_*` 生成へ接続。

#### B-02 [高] `src/jma_rainfall_pipeline/controller/weather_data_controller.py`
- 現状:
  - hourly/10minで `fetch_start = start - 1day`。
  - Parquet向けに `source_start = start + 1h` フィルタ。
- 問題:
  - 取得補正・保存補正が分離し、責務が曖昧。
- 修正:
  - 「取得窓拡張」と「最終境界フィルタ」を共通契約へ統合。
  - コントローラ独自時刻補正を除去。
- 副作用:
  - 既存出力件数に差分が出る可能性。

#### B-03 [高] `src/jma_rainfall_pipeline/exporter/parquet_exporter.py`
- 現状:
  - `_extract_observed_at` で `23:59:59.999999` 補正と `-1h` を実施。
- 問題:
  - exporter層で時刻解釈している。
- 修正:
  - exporterは「正規化済み入力を保存するだけ」に限定。
  - `period_*` 保存対応。

#### B-04 [中] `src/jma_rainfall_pipeline/exporter/csv_exporter.py`
- 現状:
  - `_normalize_time_column` が複数形式を吸収。
- 問題:
  - 出力層が時刻正規化責務を持つ。
- 修正:
  - 表示フォーマット変換のみへ縮退。
- 決定:
  - CSV向け中間データの日時表現は `datetime` 型に固定する。

### 4.3 river_meta.rainfall 系

#### C-01 [高] `src/river_meta/rainfall/domain/models.py`
- 現状:
  - `RainfallRecord` は `observed_at` のみ。
- 問題:
  - 区間情報を保持できない。
- 修正:
  - `period_start_at`/`period_end_at` を追加。
  - 互換期間は `observed_at` を `period_end_at` 同義として扱う。

#### C-02 [高] `src/river_meta/rainfall/domain/normalizer.py`
- 現状:
  - `normalize_observed_at` は単一時刻補正のみ。
- 問題:
  - 区間生成責務が未実装。
- 修正:
  - `normalize_period(...)` を新設し `period_*` を生成。
  - `24:00`/疑似24時の統一変換をここに集約。

#### C-03 [高] `src/river_meta/rainfall/sources/jma/adapter.py`
- 現状:
  - `_align_hourly_timestamp_to_waterinfo` で `-1h`。
- 問題:
  - ソース固有補正が残存。
- 修正:
  - `-1h` を廃止し共通normalizerへ委譲。
  - query境界判定は `period_*` ベースへ。

#### C-04 [高] `src/river_meta/rainfall/sources/water_info/adapter.py`
- 現状:
  - `datetime` を `normalize_observed_at` へ渡す。
- 問題:
  - 取得窓拡張と境界最終フィルタの規約が明示されていない。
- 修正:
  - request計画を明示化し、`period_*` ベースで統一判定。

#### C-05 [高] `src/river_meta/rainfall/storage/parquet_store.py`
- 現状:
  - 共通スキーマは `observed_at` 中心。
  - `quality` は `normal`/`missing` 補完。
- 問題:
  - `period_*` と `request_failed` の概念がない。
- 修正:
  - 新列追加と互換読込を実装。
  - `request_failed` はレコード化せず、別結果チャネルへ。
- 決定:
  - `quality` の許容値は増やさない（`normal` / `missing` のまま維持）。

#### C-06 [中] `src/river_meta/rainfall/outputs/analysis.py`
- 現状:
  - `observed_at` reindexで欠測補完。
- 問題:
  - 区間契約へ移行時に index軸定義を再設計要。
- 修正:
  - `period_end_at` を主時刻として再index。
  - 欠測判定を `missing` のみで算出。

#### C-07 [中] `src/river_meta/rainfall/outputs/excel_exporter.py`
- 現状:
  - 表示時刻を `dt.hour + 1` で1-24化。
- 問題:
  - 区間契約下では「どの端点を表示するか」を固定する必要。
- 修正:
  - `period_end_at` 表示を標準化。
  - 独自の24時表示ロジックは持たず、共通変換（翌日 `00:00` を24時相当として扱う）を利用する。

#### C-08 [中] `src/river_meta/rainfall/sources/jma/availability.py`
- 現状:
  - `reason=f"request_failed:{...}"` を返す経路あり。
- 評価:
  - `request_failed` の概念導入に活用可能。
- 修正:
  - 全sourceで同等の失敗コード体系へ統一。

### 4.4 hydrology_graphs 系

#### D-01 [高] `src/hydrology_graphs/io/parquet_store.py`
- 現状:
  - `observed_at` 正規化と旧JMA補正を内部実施。
- 問題:
  - 読込側に補正が残ると責務が逆流する。
- 修正:
  - 保存側統一後は読込側補正を縮退。
  - 新列 `period_*` を受理し、暫定的に `observed_at` 互換維持。

#### D-02 [中] `src/hydrology_graphs/domain/logic.py`
- 現状:
  - `observed_at` を基準にイベント窓判定。
- 修正:
  - `period_end_at` 主体へ段階移行。
- 詳細:
  - 候補A: `period_start_at` 基準
    - 長所: 区間の開始起点を揃えやすい。
    - 短所: 24時相当（翌日00:00）の扱いで、日付境界とユーザー認知がずれやすい。
  - 候補B: `period_end_at` 基準（推奨）
    - 長所: 「24時=翌日00:00」の契約と整合し、集計・表示・イベント窓の端点が統一しやすい。
    - 長所: 既存 `observed_at` を `period_end_at` 同義で移行でき、後方互換設計が単純。
    - 短所: 一部の既存集計で「開始時刻基準」の前提を見直す必要がある。
  - 本計画では候補Bを採用し、イベント窓の照合時刻は `period_end_at` に統一する。

#### D-03 [中] `src/hydrology_graphs/render/plotter.py`
- 現状:
  - x軸を `observed_at` で描画。
- 修正:
  - `period_end_at`（または要件確定の端点）を使用。

### 4.5 テスト群

#### E-01 [高] `tests/water_info/*`
- 現状:
  - `display_dt` や `drop_last_each` 前提テストあり。
- 修正:
  - 契約更新後に再定義が必要。

#### E-02 [高] `tests/river_meta/*`
- 現状:
  - `_align_hourly_timestamp_to_waterinfo` 依存テストあり。
- 修正:
  - 共通normalizer/period生成テストへ置換。

#### E-03 [中] `tests/hydrology_graphs/*`
- 現状:
  - `observed_at` 単独契約前提。
- 修正:
  - Parquet契約更新後の回帰網へ更新。

## 5. 修正パッケージ案（実施単位）

### P1: 契約基盤
- 変更:
  - `river_meta.rainfall.domain.models`
  - `river_meta.rainfall.domain.normalizer`
  - 契約定義ドキュメント（current側）
- 完了条件:
  - `period_*` の型と生成規約が確定。

### P2: 取得/パース統一
- 変更:
  - `water_info/service/flow_fetch.py`
  - `river_meta/rainfall/sources/{jma,water_info}/adapter.py`
  - `jma_rainfall_pipeline/controller/weather_data_controller.py`
  - `jma_rainfall_pipeline/parser/*`（必要最小）
- 完了条件:
  - request窓拡張と最終境界フィルタが共通化。

### P3: 永続化統一
- 変更:
  - `water_info/entry.py`
  - `river_meta/rainfall/storage/parquet_store.py`
  - `jma_rainfall_pipeline/exporter/parquet_exporter.py`
  - `hydrology_graphs/io/parquet_store.py`
- 完了条件:
  - 新Parquet契約で保存・読込が成立。
  - 旧契約の互換読込が成立。

### P4: 出力統一
- 変更:
  - `river_meta/rainfall/outputs/{analysis,excel_exporter,chart_exporter}.py`
  - `water_info/service/flow_write.py`
  - `hydrology_graphs/render/plotter.py`
- 完了条件:
  - 表示生成が `period_*` ベースに移行。

### P5: UI/結果表示統一
- 変更:
  - `hydrology_graphs/ui/*`
  - 必要なら `river_meta/rainfall/gui/*`
- 完了条件:
  - `missing` と `request_failed` の表示分離。

### P6: 旧ロジック撤去
- 変更:
  - `drop_last_each` 依存経路
  - source別 `-1h` 補正
  - `display_dt` 逆算保存
- 完了条件:
  - 局所補正コードが主経路から消える。

## 6. 既知リスク
- RISK-01: 既存Parquetとの互換不全
  - 対策: 読込互換 + 再保存正規化 + 比較検証
- RISK-02: 出力件数変化による利用者混乱
  - 対策: 差分レポート（変更前後の件数/先頭末尾時刻）
- RISK-03: モジュール間で端点定義が不一致
  - 対策: `period_end_at` 基準をドキュメント/テストで固定

## 7. [要確認] 一覧（判断保留）
- 現時点で判断保留項目はなし。

## 8. [確定] 一覧（レビュー反映）
- D-01 [確定]:
  - `water_info/postprocess.py` は主経路で使用継続しない（必須改修対象から除外）。
- D-02 [確定]:
  - CSV向け中間データの日時表現は `datetime` 型に固定する。
- D-03 [確定]:
  - `quality` の許容値は `normal` / `missing` のみを維持する。
- D-04 [確定]:
  - Excelの24時表示は独自ロジックを持たず、共通変換（翌日00:00を24時相当）を使用する。
- D-05 [確定]:
  - `hydrology_graphs` のイベント窓判定は `period_end_at` 基準で統一する。
- D-06 [確定]:
  - `request_failed` の保管先は実行ログのみとし、専用ファイル出力は行わない。
- D-07 [確定]:
  - `1day` の `period_end_at` は翌日 `00:00:00` とする（当日 `23:59:59.999999` は不採用）。
- D-08 [確定]:
  - `display_dt` は廃止する。内部契約・保存契約・中間契約に保持しない。

## 9. `display_dt` 廃止の詳細方針

### 9.1 廃止対象
- `src/water_info/infra/dataframe_utils.py` の `display_dt` 生成
- `src/water_info/service/flow_write.py` の `display_dt` 前提処理
- `src/water_info/infra/excel_summary.py` の `display_dt` 参照
- `src/water_info/entry.py` の `display_dt - 1h` 逆算保存
- `tests/water_info/*` の `display_dt` 前提アサーション

### 9.2 置換ルール
- 旧 `display_dt` の用途は次で置換する。
  - 表示時刻: `period_end_at` から生成
  - 年判定: `period_end_at.year`
  - 日次集計: `period_end_at` から日付を生成
  - Parquet保存: `period_start_at`/`period_end_at` を直接保存
- 「表示のための+1h列」は常設列として持たない。必要時に出力直前で一時生成する。

### 9.3 境界データの扱い（年またぎ対策）
- 余分取得（前月/翌月）は許容するが、正規化後に共通フィルタで切り落とす。
- 共通フィルタ条件:
  - `period_start_at >= user_start_at`
  - `period_end_at <= user_end_at`
- このフィルタ結果を Parquet/CSV/Excel/Graph すべてで共通利用する。

### 9.4 実装順（廃止に特化）
1. `period_*` をモデルと保存契約へ追加
2. 保存時の `display_dt` 逆算を削除
3. Excel/集計ロジックを `period_end_at` ベースへ置換
4. `display_dt` 参照テストを新契約テストへ置換
5. `display_dt` 列生成コードを物理削除

### 9.5 完了判定
- `rg -n "display_dt" src/water_info tests/water_info` の結果が、互換注記や移行ドキュメントを除き0件。
- 出力件数・先頭末尾時刻が境界契約（`period_*`）と一致。

## 10. 実装着手前チェックリスト
- 要件文書の確定事項（D-01〜D-08）が反映済みである。
- 契約変更に対する移行方針（後方互換期間）が合意されている。
- P1〜P6 の実施順で担当とレビュー単位が決まっている。
