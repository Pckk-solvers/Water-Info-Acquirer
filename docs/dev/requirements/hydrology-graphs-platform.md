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
  - 軽量スキャンではファイル名からメトリクスを推定し、観測所行に `雨量 / 流量 / 水位` の表示候補を持たせる
  - 気象庁は雨量のみを扱うため、気象庁の行は雨量として表示する
  - 実行前検証/プレビュー/バッチ実行時に必要な詳細読込を遅延実行
  - チェック行の表示例:
    - `観測所A (気象庁:001) / 雨量`
    - `観測所B (水文水質DB:002) / 流量`
    - `観測所C (水文水質DB:003) / 水位`
- 実行前検証:
  - 観測所はチェック済み対象のみ使用
  - 観測所チェックが未反映の場合は検証開始前に警告する
  - イベント系は `基準日 × 窓(3/5)` ごとに展開して検証し、終端側に1時間の余白を常時加える
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
  - `対象` 列は内部IDではなく日本語表示を使う
  - 表示例: `高幡橋（気象庁:111） / ハイドログラフ（水位） / 2025-01-02 / 3日窓`
  - 行をダブルクリックすると出力先フルパスを表示

## 4. スタイル調整タブ（現行動作）
- スタイル編集対象:
  - 9種（イベント3種×3日/5日 + 年最大3種）を切替
- 共通設定:
  - `24時表記` / `datetime表記` を切替できる
  - `x_axis.range_margin_rate` で X軸データ範囲マージン率を設定できる（既定 `0`）
  - 共通設定はスタイル調整タブの左側、`スタイル編集対象` の直下に配置する
  - 表示モードは `style JSON` に保存し、読み込み時に復元する
  - 選択した表示モードに応じて、プレビューおよびサンプル出力の時刻表示を切り替える
  - `datetime` 表記は通常の `00:00` 境界を使う
  - イベント系の表示データ範囲は、`datetime` / `24時表記` に関わらず終端+1時間余白を常時適用する
  - 表示モードは Parquet の保存契約を変えず、画面表示・描画の見え方だけを切り替える
  - 表示モードの選択は style JSON の `display.time_display_mode` を正とする
- 編集手段:
  - フォーム編集 + JSON直接編集（相互同期）
  - フォーム編集項目に `X軸範囲マージン率` を含める（`x_axis.range_margin_rate`）
  - フォーム編集項目に `日付境界線表示` を含める（`graph_styles.<key>.x_axis.date_boundary_line_enabled`）
  - フォーム編集項目に `日付境界線オフセット(時間)` を含める（`graph_styles.<key>.x_axis.date_boundary_line_offset_hours`）
  - `日付境界線表示` / `日付境界線オフセット(時間)` のツールチップで、どちらも個別設定であることを案内する
  - `反映` または Enter でフォーム値をスタイルへ適用
  - Undo/Redo（`Ctrl+Z`, `Ctrl+Y`, `Ctrl+Shift+Z`）
- プレビュー:
  - `プレビュー出力対象` に 観測所 / 基準日 / `対象グラフ` を置く
  - `プレビュー更新` ボタンは `プレビュー出力対象` の一番右に配置する
  - 2行目の説明ラベルは不要にし、表示しない
  - 観測所/基準日は `実行前検証でOKになった候補` から選択し、候補更新時に無効値は先頭候補へ戻す
  - `対象グラフ` は観測所/基準日と組になって有効な候補だけを出す
  - 対象候補は `precheck OK` の実在組み合わせから絞り込む
  - プレビュー解決は `観測所 / 基準日 / 対象グラフ` の完全一致だけを許可し、一致しない場合は別候補へフォールバックしない
  - `プレビュー更新` で再描画
- 開発者モード:
  - `サンプル出力` ボタンを表示
  - 保存先: `outputs/hydrology_graphs/dev_preview_samples/YYYYMMDD/*.png`

## 5. データ要件（現行）
- イベント系:
  - 必須 `interval=1hour`
  - `water_info` / `jma` の hourly データは datetime 正規化済みを前提とする
  - 対象窓（3日/5日）で欠損なし。24時相当を含む出力に対応するため、終端+1時間分を常時追加で確認する（`value` 欠損、`quality=missing` はNG）
- 表示モード:
  - `24時表記` と `datetime表記` は表示モードで、時刻ラベル表示のみ切り替える
  - `24時表記` の X 軸ラベルは時分ではなく時だけを表示する
  - style JSON の `display.time_display_mode` が保存値の正本で、未指定なら `datetime` を既定とする
- 日付境界線:
  - style JSON の `graph_styles.<key>.x_axis.date_boundary_line_enabled` を正本とする
  - 未指定時は `false` を既定として扱う
  - オフセットは style JSON の `graph_styles.<key>.x_axis.date_boundary_line_offset_hours` を正本とする
  - 未指定時の既定値は `0.0` とする
  - `date_boundary_line_enabled=true` の場合、対象グラフに日付境界線を描画する
  - 境界線の基準位置は `datetime` の日付境界 `00:00` とする
  - 実際の描画位置は `00:00 + graph_styles.<key>.x_axis.date_boundary_line_offset_hours` で決める
- X軸範囲マージン率:
  - style JSON の `graph_styles.<key>.x_axis.range_margin_rate` を正本とする
  - 未指定時は `0` を既定として扱う
  - 値は `0` 以上の数値のみ許可する
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
- スタイルJSONの正本スキーマは `src/hydrology_graphs/io/schemas/style_schema_2_0.json` とする
- style 読込/保存時は正本スキーマで検証し、互換正規化（例: `display.time_display_mode`）を適用する

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
- [style-json-schema-design.md](../reference/hydrology-graphs-platform/style-json-schema-design.md)
- [parquet-contract.md](../reference/hydrology-graphs-platform/parquet-contract.md)
- [threshold-contract.md](../reference/hydrology-graphs-platform/threshold-contract.md)

