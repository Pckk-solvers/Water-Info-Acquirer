# 雨量データ取得 詳細設計（取得順制御 + 停止時Parquet保持）

## 1. 目的
- `requirements-order-and-cancel-policy.md` の機能要件（R-01〜R-06）を、実装可能なレベルへ具体化する。
- 変更範囲を `run_rainfall_analyze()` 中心に限定し、既存の取得・保存ロジックを壊さずに拡張する。

## 2. 対象要件トレーサビリティ
| 要件 | 設計反映先 |
|---|---|
| R-01 取得順制御フラグ | `RainfallRunInput` へ `collection_order` 追加、ジョブ生成関数で順序を切替 |
| R-02 停止時Parquet保持 | 停止時ロールバック削除を行わない。保持ログのみ出力 |
| R-03 欠け月再取得 | 既存の `parquet_exists` / 月単位保存を変更しない |
| R-04 年可用性判定整合 | JMA年可用性判定をジョブ生成前に適用 |
| R-05 UI/CLI反映 | GUIラジオ追加、CLI引数追加 |
| R-06 ログ要件 | 実行開始時の `collection_order`、停止時保持方針ログ |

## 3. 現行実装の整理
- 取得対象は `run_rainfall_analyze()` 内で `stations_to_process` を作成し、`for station -> for year` で処理している。
- JMAは `fetch_available_years_hourly()` による年フィルタを観測所ループ内で適用している。
- 停止時は新規Parquet削除ではなく保持ログを出す実装に変更済み。
- GUIは `CollectTab.build_run_input()` で `RainfallRunInput` を構築しているが、取得順設定は未実装。
- CLIは `--year` / `--start-at` などはあるが、取得順引数は未実装。
- 本設計では「GUIとCLIの同等仕様」を必須とし、CLIも取得順制御に正式対応させる。

## 4. 詳細設計

## 4.1 設定モデル拡張
- `src/river_meta/services/rainfall.py`
  - `RainfallRunInput` に `collection_order: str = "station_year"` を追加する。
  - 受理値は `station_year` / `year_station`。
- 正規化関数を追加する。
  - 例: `_normalize_collection_order(value: str) -> str`
  - 未指定は `station_year`。
  - 不正値は `ValueError`（fail-fast）とし、誤設定を早期検知する。
- 年範囲正規化関数を追加する。
  - 例: `_resolve_target_years(config: RainfallRunInput) -> list[int]`
  - `years`/`year` 指定時はそれを優先。
  - `start_at/end_at` 指定時は `range(start_at.year, end_at.year + 1)` に正規化。
  - これによりGUI/CLIともに `collection_order` の適用単位を「年」に統一する。

## 4.2 処理単位の明確化（ジョブモデル）
- `source × station_key × year` を1ジョブとして扱う。
- 内部専用データ構造を追加する。
  - 例: `_CollectionJob(source_type, station_key, station_name, station_obj_list, year)`
- ジョブ生成関数を追加する。
  - 例: `_build_collection_jobs(...) -> tuple[list[_CollectionJob], int, int]`
  - 戻り値は `jobs`, `jma_requested_year_total`, `jma_filtered_year_total`。

## 4.3 ジョブ生成アルゴリズム
1. `resolved_sources` は既存どおり `["jma", "water_info"]` の安定順を維持する。
2. 各観測所に対して対象年リストを決定する（`_resolve_target_years()` の結果を利用）。
3. JMAのみ `fetch_available_years_hourly()` を適用し、対象年を絞る。
4. `source × station_key × year` ジョブを列挙する。
5. `collection_order` に応じてソートする。
   - `station_year`: `(source, station_key, year)`
   - `year_station`: `(year, source, station_key)`
6. この時点で処理順が確定するため、実行ループはジョブリストを順次消化するだけにする。

## 4.4 `run_rainfall_analyze()` の変更点
- 既存の二重ループ（station -> year）を、ジョブループ1本に置換する。
- 各ジョブで以下を実施する。
  - `source == "jma"`: `_fetch_jma_year_monthly(...)`
  - `source == "water_info"`: `_fetch_waterinfo_year(...)`
- 集計・Excel/Chart出力は現行条件を維持する。
- キャンセル判定は既存同様、ジョブ開始前・取得後・出力前後で評価する。

## 4.5 停止時Parquet保持ポリシー
- 停止時は新規Parquetを削除しない（固定）。
- `created_parquet_paths` は保持件数ログ出力のみに利用する。
- `_rollback_created_parquets()` が未使用であれば削除する。
  - 削除しない場合は「未使用」であることをコメントで明示する。

## 4.6 GUI反映（CollectTab）
- `src/river_meta/rainfall/gui.py`
  - 「対象年」の次に「取得順序」フレームを追加する。
  - ラジオ2択:
    - `観測所ごと（既定）` -> `station_year`
    - `年ごと` -> `year_station`
  - `build_run_input()` で `collection_order` を `RainfallRunInput` へ設定する。
  - `set_enabled()` でラジオの有効/無効を切替える。

## 4.7 CLI反映
- `src/river_meta/rainfall/cli.py`
  - `--collection-order` を追加（choices: `station_year`, `year_station`、既定 `station_year`）。
  - `_build_run_input()` で `RainfallRunInput.collection_order` に反映する。
  - `analyze` で `start_at/end_at` 指定時もサービス層で年範囲へ正規化されるため、GUIと同一の順序制御が適用される。

## 4.8 ログ設計
- 実行開始時:
  - `"[collect] collection_order=station_year"` のように1行出力。
- 停止時:
  - 既存方針どおり `"[Parquet] 停止時も N 件の新規Parquetを保持します。"` を出力。
- JMA年可用性の観測所別・全体ログは現行維持。

## 5. テスト設計

## 5.1 単体テスト（サービス）
- `tests/river_meta/test_rainfall_analysis.py`
  - `collection_order=station_year` で処理順が観測所優先になること。
  - `collection_order=year_station` で処理順が年優先になること。
  - `source=both` でも `source` の安定順（`jma -> water_info`）が保たれること。
  - 停止時に「保持ログ」が出ること（削除処理が走らないこと）。

## 5.2 単体テスト（CLI）
- `tests/river_meta/test_rainfall_cli.py`
  - `--collection-order year_station` が `RainfallRunInput` に設定されること。
  - 未指定時に `station_year` になること。
  - `--start-at/--end-at` 指定時でも `run_rainfall_analyze()` で年範囲正規化され、順序制御が有効であること。

## 5.3 回帰
- `uv run pytest tests/river_meta -q`

## 6. 後方互換性
- 既定値を `station_year` にすることで現行の処理順と互換を維持する。
- Parquet形式・保存先・欠け月再取得ロジックは変更しない。
- CLIは引き続き `year` と `start_at/end_at` の両指定方式を受け付けるが、内部処理は年単位へ統一される。

## 7. 実装時の注意
- ジョブ順のみを変更し、取得対象集合を変えないこと。
- JMA年可用性判定の呼び出し回数を増やさないこと（観測所単位1回を維持）。
- キャンセル判定点を減らさないこと。
