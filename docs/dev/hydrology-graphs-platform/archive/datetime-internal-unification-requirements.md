# 時刻契約統一: 内部表現 datetime 統一 要件定義

Status: archived
Updated: 2026-04-06
Related:
- `datetime-internal-unification-impact-analysis.md` (Where)
- `datetime-internal-unification-task-breakdown.md` (How/When)

## 1. 目的
- ユーザー入力から取得、Parquet保存、グラフ生成、Excel出力までを単一の時刻契約で統一する。
- 24時表現を見た目変換ではなくデータ契約として正しく扱い、時刻ずれ・欠落・重複を防止する。
- 既存の局所補正（個別の `-1h`、末尾切り捨て等）に依存しない一貫動作へ移行する。

## 2. スコープ
- 対象:
  - `water_info` / `jma_rainfall_pipeline` / `river_meta.rainfall` / `hydrology_graphs` の時刻取り扱い
  - 入力期間解釈、取得境界、内部保持、Parquet、グラフ、Excelの時刻契約
  - 旧データ互換方針と移行方針
- 非対象:
  - グラフの装飾・配色・線種など時刻以外の描画仕様
  - UIレイアウト変更（操作導線追加など）

## 3. 基本方針
- 内部表現は常に `datetime` を使用する。文字列時刻や疑似時刻（例: `24:00` 文字列）を内部状態に保持しない。
- 値の意味は `instantaneous`（瞬間値）と `interval`（区間値）を分離して扱う。
- `water_info:S/R` は瞬間値、`water_info:U` と `JMA rainfall` は区間値として扱う。
- 24時は `period_end_at == 翌日00:00:00` で表現する。
- 外部表示（Excel/GUI）も内部契約に基づいて生成し、表示専用のごまかし変換に依存しない。

## 4. 機能要件

### R-DTU-01 単一時刻契約
- 全モジュールで次の共通契約を採用する。
  - `observed_at`: 観測値の代表時刻（瞬間値は観測時刻、区間値は原則 `period_end_at`）
  - `period_start_at`: 観測区間の開始（区間値で使用）
  - `period_end_at`: 観測区間の終了（区間値で使用）
  - `interval`: `10min` / `1hour` / `1day`
- `period_start_at`/`period_end_at` の必須条件は値意味に従う。
  - 瞬間値 (`water_info:S/R`): `period_*` は `NULL` 許容
  - 区間値 (`water_info:U`, `JMA rainfall`): `period_*` は非 `NULL` 必須

### R-DTU-02 ソース正規化
- JMAの `1-24時` は取得直後に `datetime` 区間へ正規化する。
- water_infoは mode ごとに正規化する。
  - `S/R`: 瞬間値として正規化
  - `U`: 区間値として正規化
- 正規化ロジックは各モジュールに分散実装せず、共通関数として一元管理する。

### R-DTU-03 24時のデータ表現
- 24時相当データは内部で翌日 `00:00:00` を使って保持する。
- 24時をUI/Excelで表示する必要がある場合も、元データは `datetime` 区間から生成する。
- 24時相当を表すために `23:59:59.999999` などの擬似値へ変換しない。

### R-DTU-04 Parquet契約更新
- 共通Parquetスキーマへ `period_start_at`/`period_end_at` を追加する。
- 新規保存時は必ず新契約で保存する。
- 旧契約読込時は互換読込を許容するが、再保存時は新契約へ正規化する。
- Parquet時刻列の型は次で固定する。
  - `observed_at`: `timestamp[ns]`（naive, local運用）
  - `period_start_at`: `timestamp[ns]`（naive, local運用）
  - `period_end_at`: `timestamp[ns]`（naive, local運用）
- Null許容は次で固定する。
  - `observed_at`: 非null必須（互換期間を除く）
  - 瞬間値 (`water_info:S/R`): `period_start_at` / `period_end_at` は `NULL` 許容
  - 区間値 (`water_info:U`, `JMA rainfall`): `period_start_at` / `period_end_at` は非 `NULL` 必須
- 旧形式読込時は次で扱う。
  - 区間値: `observed_at` を `period_end_at` 同義として扱い、`period_start_at` を `interval` から導出する
  - 瞬間値: `observed_at` を優先し、`period_*` は `NULL` を許容する

### R-DTU-05 出力契約更新
- グラフ生成は `period_*` を入力として時刻軸を構成する。
- Excel出力は `datetime` 契約を元に出力し、表示フォーマットは派生値として生成する。
- 出力層独自の時刻補正（例: 固定 `-1h`、末尾削除）を禁止する。

### R-DTU-06 期間境界定義
- ユーザー指定の開始・終了 `datetime` に対する包含条件を明文化する（閉区間/半開区間を統一）。
- 取得層、保存層、出力層すべてで同一境界定義を使用する。
- 本要件では観測区間を半開区間 `[period_start_at, period_end_at)` と定義する。
- ユーザー指定期間は半開区間 `[user_start_at, user_end_at)` と定義する。
- 最終出力へ渡す共通フィルタ条件は値意味に従って次で固定する。
  - 瞬間値: `user_start_at <= observed_at <= user_end_at`
  - 区間値: `user_start_at <= period_end_at <= user_end_at`
- 日次（`1day`）の1レコードは次で扱う。
  - 区間値: `period_start_at=当日00:00:00`、`period_end_at=翌日00:00:00`
  - 瞬間値: `observed_at` を時刻キーとし `period_*` は `NULL` 許容

### R-DTU-07 廃止対象の明確化
- 次の局所補正は最終的に廃止する。
  - ソース別の個別 `-1h` 補正
  - `drop_last_each` 等の暗黙トリミング
  - 表示専用列からの逆算による保存時刻生成
  - `display_dt` のような表示専用中間列の常設保持

### R-DTU-08 リクエスト境界ルール（取得）
- ユーザー指定の `start_at` / `end_at` は内部の絶対基準とし、取得層はこの範囲を欠落なく再現できるようにリクエストを組み立てる。
- 取得窓の拡張は「正規化のために必要な場合のみ」許可する。拡張した場合でも最終確定データは `start_at` / `end_at` の契約境界で必ず再フィルタする。
- 1時間データは次を最低限保証する。
  - 指定開始が `00:00` の場合、月初・年初を跨いでも先頭区間が欠落しない。
  - 指定終了が日末/24時相当を含む場合、末尾区間が欠落しない。
- ソース固有のURL/パラメータ差（JMA, water_info）は許容するが、境界解釈は共通契約で統一する。

### R-DTU-09 パース正規化ルール
- パース層はソースHTML/CSV由来の時刻表現を受け取り、次の正規化を行う。
  - `24:00` は翌日 `00:00:00` に正規化する。
  - `23:59:59.999999` 等の疑似24時は翌日 `00:00:00` に正規化する。
  - 欠測・異常時刻は `invalid` として検出し、件数と理由をログ可能にする。
- 正規化後は `interval` ごとに `period_start_at` / `period_end_at` を生成する。
  - `1hour`: `period_end_at - 1h == period_start_at`
  - `10min`: `period_end_at - 10min == period_start_at`
  - `1day`: `period_end_at - 1day == period_start_at`（`period_end_at` は翌日 `00:00:00`）
- パース層で時刻変換した結果は、後段で再変換しない（単方向変換）。

### R-DTU-10 責務分離（request/parse/normalize）
- request層責務:
  - ユーザー期間を満たす取得窓の計画
  - API/HTML取得の再試行と失敗通知
- parse層責務:
  - 生データから時刻/値を抽出し、契約時刻へ正規化
- normalize層責務:
  - 共通スキーマ化（`observed_at`, `period_*`, `interval`, `quality`）
  - ソース差分を吸収した上で永続化可能なレコードを生成
- 出力層（graph/excel）は正規化済みデータの利用に限定し、時刻解釈ロジックを持たない。

### R-DTU-12 `display_dt` 廃止と出力生成
- `display_dt` は内部契約・保存契約の列として保持しない。
- 表示用時刻が必要な場合は、値意味に応じて出力直前に派生生成する。
  - 瞬間値: `observed_at`
  - 区間値: `period_end_at`
- Parquet/CSV/Excel/Graph に渡す前データは、共通境界フィルタ適用後の同一レコード集合を使用する。
- 共通境界フィルタは値意味に従って適用する。
  - 瞬間値: `user_start_at <= observed_at <= user_end_at`
  - 区間値: `user_start_at <= period_end_at <= user_end_at`
- 24時表示が必要な場合の表示規約を次で固定する。
  - 表示対象時刻が `00:00:00` かつ「直前1日区間の終端」である場合のみ `24:00` として表示してよい。
  - それ以外の表示は `00:00` を維持する。
  - 24時表示判定は共通関数1箇所で実装し、Excel/GUIで同一関数を利用する。

### R-DTU-11 欠測と取得失敗の区別
- `missing` は「取得は成功したが、対象時刻の値が存在しない/補完不能」の場合にのみ使用する。
- `request_failed` は「観測所・期間の取得処理自体が成立しない」場合に使用する。
  - 例: HTTP失敗、タイムアウト超過、認証/権限失敗、ソースページ構造崩壊、パース不能
- 観測所・期間が `request_failed` の場合:
  - 通常の時系列レコードとしては保存しない（欠測レコード化しない）。
  - 実行結果に失敗として明示し、失敗理由をログ・結果一覧で追跡可能にする。
- 取得成功後の部分欠落（先頭/末尾含む）は `missing` レコードとして保持する。
- `request_failed` 実行ログの最低項目を次で固定する。
  - `run_id`
  - `source`（`jma` / `water_info`）
  - `station_key`
  - `interval`
  - `user_start_at`
  - `user_end_at`
  - `error_code`
  - `error_message`
  - `retry_count`
  - `occurred_at`
- `request_failed` は `logs/` 配下の実行ログにのみ保存し、Parquet/CSV/Excelには保存しない。

## 5. 非機能要件
- 大量データ時の性能劣化は次を上限とする。
  - 実行総時間: 現行比 `+15%` 以内
  - ピークメモリ: 現行比 `+20%` 以内
  - 測定対象: 年跨ぎ1年分（hourly）・複数観測所バッチ実行
- 正規化処理は決定的であり、同一入力に対して常に同一結果となる。
- 変換ログはトレース可能で、異常レコードの原因追跡が可能であること。

## 6. 受け入れ条件
1. 全対象モジュールで内部時刻型が `datetime` 契約に統一されている。
2. 24時相当データが `period_end_at=翌日00:00:00` として一貫して扱われる。
3. ユーザー指定期間に対し、取得件数と出力件数の境界挙動が一致する。
4. 旧Parquetを読み込んでも、新規保存時に新契約へ正規化される。
5. 同一サンプルに対し、JMA/water_infoの経路差による時刻ずれが発生しない。
6. 既存の局所補正コードが削除または無効化され、共通正規化へ置換されている。
7. リクエスト境界拡張がある場合でも、最終データは契約境界外レコードを含まない。
8. パース層で `24:00` / 疑似24時が同一ルールで正規化される。
9. 取得失敗ケースは `missing` と混同されず、`request_failed` として別扱いになる。

## 7. テスト観点（最小）
- 境界:
  - 月初 `00:00`
  - 月末/日末の24時相当（翌日 `00:00`）
  - 年跨ぎ
- request/parse:
  - 取得窓拡張あり/なし双方で先頭末尾欠落がないこと
  - `24:00`, `23:59:59.999999`, `hour=24` の同値正規化
  - 境界外レコードが最終データに混入しないこと
  - 取得完全失敗は `request_failed`、部分欠落は `missing` で記録されること
- 整合:
  - 取得 → Parquet → グラフ → Excel で時刻が一致
  - 新旧Parquet混在時の互換読込と再保存
- 回帰:
  - 欠測判定、集計窓、イベント窓判定の動作維持
  - 既存UI操作での実行失敗増加がないこと

## 8. 実装フェーズ（要求レベル）
1. 契約定義フェーズ: datetime契約と境界定義を文書化しレビュー合意
2. 正規化共通化フェーズ: 変換関数を一本化
3. 保存・読込フェーズ: Parquet契約更新と互換導入
4. 出力フェーズ: グラフ/Excelの時刻入力を新契約へ移行
5. 移行完了フェーズ: 局所補正の撤去と回帰テスト完了

## 9. レビュー確定事項（2026-04-01）
- CSV向け中間データの日時表現は `datetime` 型に固定する。
- `quality` の許容値は `normal` / `missing` のみを維持し、`request_failed` はレコード化しない。
- 24時表示は独自ロジックを持たず、共通変換（翌日 `00:00` を24時相当）を利用する。
- `hydrology_graphs` のイベント窓判定は `period_end_at` 基準で統一する。
- `request_failed` の保管先は実行ログのみとし、専用ファイル出力は行わない。
- `1day` の `period_end_at` は翌日 `00:00:00` に統一する。
- `display_dt` は廃止し、出力直前派生に統一する。

## 10. 検証コマンド固定（実装完了判定）
- 各フェーズで最低限次を実行する。
  - `uv run pytest tests/river_meta -k "normalizer or period or parquet"`
  - `uv run pytest tests/jma_rainfall_pipeline -k "parquet_exporter or hourly_table_parser"`
  - `uv run pytest tests/water_info -k "dataframe or flow or parquet"`
  - `uv run pytest tests/hydrology_graphs -k "domain or parquet_store or services"`
- 全体回帰時は次を実行する。
  - `uv run pytest tests/river_meta tests/jma_rainfall_pipeline tests/water_info tests/hydrology_graphs`

