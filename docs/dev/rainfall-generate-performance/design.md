# 整理・出力タブ 性能改善 詳細設計（Excel差分 + グラフ差分）

## 1. 目的
- `run_rainfall_generate()` の再実行コストを削減する。
- `excel_manifest.json` を導入し、Excel/グラフの差分判定を安定して行う。

## 2. 変更対象
- `src/river_meta/services/rainfall.py`
- `src/river_meta/rainfall/excel_exporter.py`（必要最小限）
- `src/river_meta/rainfall/chart_exporter.py`（スキップ判定の受け口）
- `src/river_meta/rainfall/parquet_store.py`（必要なら補助関数）

## 3. 新規ファイル
- 出力先配下に manifest を作成:
  - `output_dir/metadata/excel_manifest.json`

## 3-1. 実行オプション（確定）
- `use_diff_mode: bool`（既定: `true`）
- `force_full_regenerate: bool`（既定: `false`）
- 優先順位:
  - `force_full_regenerate=true` の場合、`use_diff_mode` を無視してフル再生成。

## 4. manifest仕様
```json
{
  "version": 1,
  "updated_at": "2026-03-05T12:34:56+09:00",
  "stations": {
    "jma::11_62001": {
      "excel_path": "excel/11_62001_TEST.xlsx",
      "years": {
        "2022": {
          "source_files": [
            {"path": "parquet/jma_11_62001_2022_01.parquet", "mtime_ns": 0, "size": 0}
          ],
          "digest": "sha256:...",
          "written_at": "2026-03-05T12:34:56+09:00"
        }
      },
      "charts": {
        "2022": {
          "1時間雨量": {"path": "charts/TEST_11_62001/2022_1時間雨量.png", "exists": true},
          "3時間雨量": {"path": "...", "exists": true}
        }
      }
    }
  }
}
```

## 5. 差分判定ルール
### 5.1 Excel
- 観測所×年ごとに `digest` を計算。
- `digest` が manifest と一致:
  - 既定では「再生成しない（skip）」。
- 不一致または未登録:
  - 対象年を再出力対象に含める。

### 5.2 グラフ
- 観測所×年×指標ごとに:
  - manifestに登録済み かつ ファイル存在 かつ 対象年の `digest` 不変なら skip。
  - それ以外は生成。

### 5.3 モード適用
- `force_full_regenerate=true`:
  - Excel/グラフとも差分判定を行わず全対象を再生成。
- `force_full_regenerate=false` かつ `use_diff_mode=true`:
  - 本設計の差分判定を適用。
- `force_full_regenerate=false` かつ `use_diff_mode=false`:
  - 互換のため全対象を再生成（manifestは更新する）。

## 6. digest計算方式
- 観測所×年の入力Parquet集合を対象に計算。
- 入力要素:
  - 相対パス
  - `mtime_ns`
  - `size`
- 上記を安定順で連結して `sha256`。
- まずは高速判定（mtime/sizeベース）を採用し、必要時に内容ハッシュへ拡張。

## 7. `run_rainfall_generate()` 再構成
### 7.1 観測所単位バッチ
- 現在の `complete_entries`（観測所×年）を観測所単位でグループ化。
- 観測所ごとに:
  - 対象年一覧
  - 各年の source_df -> timeseries/annual を生成
  - Excelは1回で出力

### 7.2 Excel差分更新
- 再出力対象年のみ集計してExcelへ反映。
- 方式は2案:
  1. 対象年だけ追記（既存年の重複回避が必要）
  2. 観測所単位でファイル再生成（実装が単純で安全）
- 初期実装は **2案（再生成）** を採用:
  - 差分判定で「変更なし観測所」は完全スキップ
  - 変更あり観測所のみ1回再生成

### 7.3 グラフ差分生成
- 観測所単位で年×指標を判定し、未生成/変更分のみ `export_rainfall_charts()` 実行。

## 8. ログ設計
- 観測所単位:
  - `excel: generated/skipped years`
  - `charts: generated/skipped images`
  - 処理時間
- 全体:
  - Excel生成件数/スキップ件数
  - グラフ生成件数/スキップ件数
  - 合計時間
  - モード情報（`diff_mode`, `force_full_regenerate`）

## 9. 失敗時方針
- manifest 書き込み失敗:
  - 出力処理は成功扱い、WARNログを出して次回はフル判定。
- manifest 読み込み失敗/破損:
  - フォールバックしてフル再生成。

## 10. テスト設計
1. manifest入出力
- 新規生成・更新・破損時フォールバック

2. Excel差分
- digest一致で観測所スキップ
- digest不一致で観測所再生成

3. グラフ差分
- 既存PNG + digest一致でスキップ
- 未生成またはdigest不一致で生成

4. 回帰
- 不完全年スキップ
- 停止要求
- 既存の `tests/river_meta` 一式通過

5. モード優先順位
- 既定（diff=ON, force=OFF）で差分判定が有効
- force=ON で差分判定が無効化される

## 11. GUI反映（最小構成）
- 整理・出力タブに2項目のみ追加:
  - `差分更新を使う`（既定ON）
  - `全再生成する`（既定OFF）
- 実行時は `全再生成する` を最優先する。
