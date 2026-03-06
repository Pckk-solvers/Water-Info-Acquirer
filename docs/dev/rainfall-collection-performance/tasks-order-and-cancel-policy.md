# 雨量データ取得 タスク分解（取得順制御 + 停止時Parquet保持）

## 1. 方針
- 詳細設計 `detailed-design-order-and-cancel-policy.md` を実装単位に分割する。
- 各タスクは「変更ファイル」「完了条件」「テスト」を持つ。
- 実装順は依存関係を優先し、早い段階で回帰確認を入れる。

## 2. タスク一覧

### T-01 設定値追加（サービス層）
- 変更ファイル
  - `src/river_meta/services/rainfall.py`
- 実施内容
  - `RainfallRunInput` に `collection_order` を追加（既定 `station_year`）。
  - 正規化/検証関数を追加し、不正値を `ValueError` とする。
  - `start_at/end_at` から対象年配列を解決する関数を追加する。
- 完了条件
  - `collection_order` 未指定で既存挙動維持。
  - `station_year` / `year_station` が明確に受理される。
  - `start_at/end_at` 指定時に年配列へ正規化できる。
- テスト
  - 既存テストが通る（この時点では追加テストなし）。

### T-02 ジョブ生成関数の新設
- 変更ファイル
  - `src/river_meta/services/rainfall.py`
- 実施内容
  - `source × station_key × year` のジョブ構造を追加。
  - JMA年可用性判定を適用したうえでジョブを作る関数を追加。
  - `collection_order` に応じたソート実装を追加。
- 完了条件
  - 両順序でジョブ列が再現可能。
  - `source=both` で `source` 安定順を維持。
- テスト
  - モックでジョブ順を検証する新規テスト追加。

### T-03 `run_rainfall_analyze()` への組み込み
- 変更ファイル
  - `src/river_meta/services/rainfall.py`
- 実施内容
  - 既存の `for station -> for year` ループをジョブループへ置換。
  - 既存の取得・保存・集計・出力処理は呼び出し順のみ変更。
  - 実行開始時ログに `collection_order` を追加。
- 完了条件
  - 取得結果集合が順序以外で変化しない。
  - 停止時挙動（中断・保持）が維持される。
- テスト
  - 順序別の実行順テスト。
  - 既存の年可用性テストの期待値更新（必要時）。

### T-04 停止時Parquet保持の整理
- 変更ファイル
  - `src/river_meta/services/rainfall.py`
- 実施内容
  - ロールバック削除処理の未使用関数を整理（削除または未使用明示）。
  - 停止時保持ログの一貫性を確認。
- 完了条件
  - 停止時にParquet削除が発生しない。
  - ログメッセージが仕様どおり。
- テスト
  - 停止シナリオのサービステスト追加/更新。

### T-05 GUI反映（取得順序ラジオ）
- 変更ファイル
  - `src/river_meta/rainfall/gui.py`
- 実施内容
  - Collectタブに取得順序ラジオを追加。
  - `build_run_input()` から `collection_order` を設定。
  - 実行中の有効/無効切替に含める。
- 完了条件
  - GUI操作で順序設定がサービス入力へ渡る。
  - 既存レイアウトが崩れない。
- テスト
  - 必要に応じて軽量UIテスト、最低限は手動確認項目化。

### T-06 CLI反映
- 変更ファイル
  - `src/river_meta/rainfall/cli.py`
  - `tests/river_meta/test_rainfall_cli.py`
- 実施内容
  - `--collection-order` 引数追加。
  - `RainfallRunInput` へ引数を反映。
- 完了条件
  - CLI未指定時は `station_year`。
  - 指定時に `year_station` が反映される。
  - `--start-at/--end-at` 指定時もサービス側の年正規化でGUI同等の順序制御になる。
- テスト
  - CLI引数の新規テスト追加。
  - `--start-at/--end-at` + `--collection-order` 併用時のテスト追加。

### T-07 テスト・回帰・最終確認
- 変更ファイル
  - `tests/river_meta/test_rainfall_analysis.py`
  - `tests/river_meta/test_rainfall_cli.py`
- 実施内容
  - 順序切替・停止保持のテストを通す。
  - 回帰実行。
- 完了条件
  - `uv run pytest tests/river_meta -q` が成功。
  - 新規要件の受け入れ条件（1〜9）に対応する検証結果が揃う。

## 3. 実行順（推奨）
1. T-01
2. T-02
3. T-03
4. T-04
5. T-05
6. T-06
7. T-07

## 4. レビュー観点
- 順序変更が「対象集合」へ影響していないか。
- 停止時に保存済みParquetが削除されないか。
- GUI/CLIの既定値が一致しているか（`station_year`）。
- 既存ログや既存テスト期待値を壊していないか。
