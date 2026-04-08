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
  - 背景色は固定（白）とし、フォーム/JSON からは設定しない
  - `Y軸数値形式` は共通フォーム1項目で編集し、ハイエトグラフでは左Y軸/右Y軸へ同一設定を適用する
  - フォーム編集項目に `X軸範囲マージン率` を含める（`x_axis.range_margin_rate`）
  - `日付境界線オフセット(時間)` 行の先頭チェックで `date_boundary_line_enabled` を切り替える（1行統合）
  - `基準線ラベルオフセット` 行の先頭チェックで `threshold.label_enabled` を切り替える（1行統合）
  - 日付境界線 / 基準線ラベルのオフセット行には、個別設定であることを案内するツールチップを付ける
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

## 9. 追加要件（未実装）: ハイエトグラフ見た目完成

### 9.1 背景
- 現行のハイエトグラフは時間雨量の棒のみで、運用時に必要な読図性（欠測視認、累積雨量、軸調整）の自由度が不足している。

### 9.2 目的
- ハイエトグラフに累積雨量線を重ねて表示し、累積側は自動で収まりよく描画する。
- 欠測区間をグレーで可視化し、データ欠落を見落としにくくする。
- 軸・目盛り・棒外枠をスタイルで調整可能にする。

### 9.3 スコープ
- 対象は `hyetograph`（3日/5日）のみ。
- `hydrograph_*` と `annual_max_*` の描画仕様は本要件では変更しない。

### 9.4 入力
- 既存の Parquet 契約（`value`, `quality`, `observed_at`/`period_end_at`）を利用する。
- style JSON に以下の追加設定を受け付ける（キー名は実装時に schema 設計へ反映）:
  - 棒幅（`bar.width`）による棒間隔見え方の調整
  - 棒外枠線（黒固定）の太さ・濃さ
  - 累積雨量折れ線の表示ON/OFF、色、線幅、線種
  - 右Y軸（累積）の上限 `max`、刻み `tick_step`
  - 左Y軸（時間雨量）の上限 `max`、刻み `tick_step`
  - グリッド表示の軸別ON/OFF（`x_grid_enabled`, `y_grid_enabled`）
  - 欠測帯（グレー）の色・透過率・表示ON/OFF
- スタイル調整フォーム（ハイエトのみ）は、各機能を1行で編集できる構成にする:
  - 左側: ON/OFFチェックボックス
  - 右側: 値編集（数値・色・線種など）
- 1行構成の対象は以下で固定する（ハイエトのみ）:
  - `累積雨量線` 行
    - 左: `cumulative_line.enabled`
    - 右: `cumulative_line.width`, `cumulative_line.style`, `cumulative_line.color`
  - `欠測帯` 行
    - 左: `missing_band.enabled`
    - 右: `missing_band.alpha`, `missing_band.color`
  - `グリッド` 行
    - 左: `grid.x_enabled`, `grid.y_enabled`（X/Yを同一行で個別チェック）
    - 右: なし（ON/OFFのみ）
  - `棒設定` 行
    - 左: 常時ON扱い（チェックなし）
    - 右: `bar_color`, `bar.width`, `bar.edge_width`, `bar.edge_alpha`
  - `系列設定` 行
    - 左: 常時ON扱い（チェックなし）
    - 右: `series_color`, `series_width`, `series_style`
  - `Y軸(時間雨量)` 行
    - 左: 常時ON扱い（チェックなし）
    - 右: `y_axis.max`, `y_axis.tick_step`

### 9.5 出力
- ハイエトグラフで以下を同時に表示する:
  - 時間雨量（棒）
  - 累積雨量（折れ線）
  - 欠測区間: グレー帯
- 同一X軸上で時刻位置を共有する。
- `Y軸数値形式` は左軸と右軸で同じ表示形式（`plain/comma/percent`）を使う。

### 9.6 制約
- 左右Y軸の下限は固定 `0`（手動変更不可）。
- 棒間隔の見え方は `bar.width` でのみ調整する（時間軸の時刻位置は変えない）。
- 棒外枠線の色は黒固定とし、色変更機能は持たない。
- 累積雨量側は細かな手動指定を前提にせず、自動上限で収める。
- 累積雨量側の補助設定は内部利用のみに留め、フォームでの細かい調整は提供しない。
- 欠測判定は `quality=missing` と、期待時刻に行が存在しない区間を対象にする。
- 欠測区間では累積線を補間しない（線を切る）。
- 既存の 24時表記 / datetime 表記の切替仕様は維持する。
- グリッドの既定値は、縦線（Xグリッド）OFF、横線（Yグリッド）ON とする。

### 9.7 完了条件
- style JSON で棒幅、棒外枠（黒固定の太さ・濃さ）、累積線、左Y軸（max/tick_step）、欠測帯を制御できる。
- ハイエトグラフのプレビュー/バッチ描画で2軸表示と欠測帯表示が反映される。
- 累積雨量線が右Y軸内に収まって表示される。
- 追加項目の schema 検証・フォーム同期・描画テストが追加され、部分テストで確認できる。

## 10. 欠測データ方針の移行要件（大規模変更）

### 10.1 背景
- 現行は、欠測を含む対象を precheck / 実行で除外（NG）しやすい設計思想になっている。
- 実運用では欠測を含むデータ自体も観測実態として扱う必要があるため、欠測を保持したまま可視化できる設計へ移行する。

### 10.2 方針
- 欠測を「処理対象外」ではなく「可視化対象」として扱う。
- 欠測を含む系列でもグラフ生成を可能にする。
- 欠測区間は視覚的に識別できる表現（例: 欠測帯、線の切断）を標準仕様にする。

### 10.3 制約
- 本方針は影響範囲が広いため、段階的に移行する。
- 要件変更の反映時は、必ず現行実装と照らし合わせて差分を明示する。
- precheck / preview / batch / render / UI の各層で、欠測の扱いが矛盾しないことを確認する。

### 10.4 完了条件（方針レベル）
- 欠測を含む入力に対して、precheck で一律排除しない設計へ移行方針が定義されている。
- 欠測の可視化ルールと、処理継続条件（どの欠測なら描画可とするか）が文書化されている。
- 実装変更時に、要件と現行実装の差分確認結果を同時に記録する運用が定義されている。

