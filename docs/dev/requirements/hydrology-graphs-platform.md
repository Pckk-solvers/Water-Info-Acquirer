# Hydrology Graphs Platform 要件（現行）


## 1. 対象範囲
- 対象実装: `src/hydrology_graphs`
- GUI: Tkinter 2タブ構成
  - 条件設定・実行
  - スタイル調整
- データ入力: 共通スキーマ Parquet（`source/station_key/...` 11列）
- 出力: PNG（バッチ出力、プレビュー、開発者モードのサンプル出力）

## 2. グラフ種別（実装固定）
- イベント系（3日/5日）:
  - `hyetograph`
  - `hydrograph_discharge`
  - `hydrograph_water_level`
- 年最大系:
  - `annual_max_rainfall`
  - `annual_max_discharge`
  - `annual_max_water_level`

## 3. 条件設定・実行タブ（現行動作）
- 入力:
  - Parquetディレクトリ
  - 基準線定義（任意、CSV/JSON）
  - グラフ種別（3行×3列のチェック式マトリクス: 雨量/流量/水位 × 3日窓/5日窓/年最大）
  - イベント系の窓補正設定（24時相当の記録を取りこぼさないための終端+1時間余白の有無）
  - 観測所（チェック式、`全選択` / `全解除`）
    - 表示: `<source_label>:<station_key> (<station_name>)`
    - `source_label`: `jma -> 気象庁`、`water_info -> 水文水質DB`
    - クリック判定は先頭のチェック記号（☐/☑）領域のみ
  - 基準日（候補プルダウンから追加、リスト管理）
    - `チェック反映` 押下時に候補日を再計算（観測所選択の和集合）
  - 基準日CSVの保存/読込（`base_date` 列）
- レイアウト:
  - 左右2ペインは1:1基準（右結果テーブルの初期幅要求を抑制）
  - 基準日設定1行目: 左に`チェック反映`、右に`追加`/`削除`
  - 操作ヘルプはツールチップで表示
- スキャン:
  - `スキャン`は軽量スキャン（観測所一覧更新）を実行
  - 実行前検証/プレビュー/バッチ実行時に必要な詳細読込を遅延実行
- 実行前検証:
  - 観測所はチェック済み対象のみ使用
  - 観測所チェックが未反映の場合は検証開始前に警告する
  - イベント系は `基準日 × 窓(3/5)` ごとに展開して検証し、窓補正設定が有効な場合は終端側に1時間の余白を加える
  - `3日` と `5日` 同時選択時は5日判定結果を3日側へ再利用して重複計算を抑制
  - 統合結果テーブルで `precheck_ng/ready` を表示
  - `ready` のみをバッチ対象として保持
- バッチ実行:
  - 実質デフォルトスタイルの場合は実行前確認ダイアログを表示
  - 出力先フォルダを選択し、`ready` 対象を一括描画
  - イベント系出力パス: `<output_dir>/<station_key>/<graph_type>/<base>/<3day|5day>/graph.png`
  - 年最大系出力パス: `<output_dir>/<station_key>/<graph_type>/annual/graph.png`
  - 実行中停止をサポート（未着手は `skipped`）
- 結果表示:
  - 列は `対象/窓/状態/理由` の4列
  - 行をダブルクリックすると出力先フルパスを表示

## 4. スタイル調整タブ（現行動作）
- スタイル編集対象:
  - 9種（イベント3種×3日/5日 + 年最大3種）を切替
- 編集手段:
  - フォーム編集 + JSON直接編集（相互同期）
  - `反映` または Enter でフォーム値をスタイルへ適用
  - Undo/Redo（`Ctrl+Z`, `Ctrl+Y`, `Ctrl+Shift+Z`）
- プレビュー:
  - 左の「スタイル編集対象」に連動して対象グラフ種別を決定
  - 観測所/基準日は `実行前検証でOKになった候補` から選択
  - `プレビュー更新` で再描画
- 開発者モード:
  - `サンプル出力` ボタンを表示
  - 保存先: `outputs/hydrology_graphs/dev_preview_samples/YYYYMMDD/*.png`

## 5. データ要件（現行）
- イベント系:
  - 必須 `interval=1hour`
  - `water_info` / `jma` の hourly データは datetime 正規化済みを前提とする
  - 対象窓（3日/5日）で欠損なし。24時相当を含む出力に対応する場合は、窓補正設定が有効なときに終端+1時間分を追加で確認する（`value` 欠損、`quality=missing` はNG）
- 年最大系:
  - `metric` 一致データから年最大を算出
  - 年最大系列が10年以上必須
- 基準線ファイル指定時:
  - 対象キー（`source+station_key+graph_type`）に一致する基準線が0件ならNG

## 6. スタイル契約（現行）
- `schema_version: "2.0"`
- ルート必須: `graph_styles`
- ルート禁止: `common`, `variants`
- `graph_styles` は必須9キー固定

詳細は [style-contract.md](../reference/hydrology-graphs-platform/style-contract.md) を参照。

## 7. 参照実装
- `src/hydrology_graphs/ui/app.py`
- `src/hydrology_graphs/ui/tabs_execute.py`
- `src/hydrology_graphs/ui/tabs_style.py`
- `src/hydrology_graphs/services/usecases.py`
- `src/hydrology_graphs/io/parquet_store.py`
- `src/hydrology_graphs/io/style_store.py`
- `src/hydrology_graphs/io/threshold_store.py`
- `src/hydrology_graphs/render/plotter.py`

## 8. 参照資料

- [style-contract.md](../reference/hydrology-graphs-platform/style-contract.md)
- [parquet-contract.md](../reference/hydrology-graphs-platform/parquet-contract.md)
- [threshold-contract.md](../reference/hydrology-graphs-platform/threshold-contract.md)

