# Hydrology Graphs Platform Current Spec

Status: current

## 1. 対象範囲
- 対象実装: `src/hydrology_graphs`
- GUI: Tkinter 2タブ構成
  - 条件設定・実行
  - スタイル調整
- データ入力: 共通スキーマ Parquet（`source/station_key/...` 9列）
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
  - グラフ種別（6種チェック）
  - 観測所（複数選択）
  - 基準日（改行区切り `YYYY-MM-DD`）
  - イベント窓（3日/5日）
- 実行前検証:
  - 選択対象を `ok/ng` 判定して一覧表示
  - `ok` のみをバッチ対象として保持
- バッチ実行:
  - 出力先フォルダを選択し、`ok` 対象を一括描画
  - 出力パス: `<output_dir>/<station_key>/<graph_type>/<base|annual>/graph.png`
  - 実行中停止をサポート（未着手は `skipped`）

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
  - 対象窓（3日/5日）で欠損なし（`value` 欠損、`quality=missing` はNG）
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

詳細は [style-contract.md](/C:/Users/yuuta.ochiai/Documents/GitHub/Water-Info-Acquirer/docs/dev/hydrology-graphs-platform/current/style-contract.md) を参照。

## 7. 参照実装
- [app.py](/C:/Users/yuuta.ochiai/Documents/GitHub/Water-Info-Acquirer/src/hydrology_graphs/ui/app.py)
- [tabs_execute.py](/C:/Users/yuuta.ochiai/Documents/GitHub/Water-Info-Acquirer/src/hydrology_graphs/ui/tabs_execute.py)
- [tabs_style.py](/C:/Users/yuuta.ochiai/Documents/GitHub/Water-Info-Acquirer/src/hydrology_graphs/ui/tabs_style.py)
- [usecases.py](/C:/Users/yuuta.ochiai/Documents/GitHub/Water-Info-Acquirer/src/hydrology_graphs/services/usecases.py)
- [parquet_store.py](/C:/Users/yuuta.ochiai/Documents/GitHub/Water-Info-Acquirer/src/hydrology_graphs/io/parquet_store.py)
- [style_store.py](/C:/Users/yuuta.ochiai/Documents/GitHub/Water-Info-Acquirer/src/hydrology_graphs/io/style_store.py)
- [threshold_store.py](/C:/Users/yuuta.ochiai/Documents/GitHub/Water-Info-Acquirer/src/hydrology_graphs/io/threshold_store.py)
- [plotter.py](/C:/Users/yuuta.ochiai/Documents/GitHub/Water-Info-Acquirer/src/hydrology_graphs/render/plotter.py)
