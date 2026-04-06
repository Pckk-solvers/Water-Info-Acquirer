# Hydrology Graphs architecture

`src/hydrology_graphs/` は、雨量・水位グラフの作成と条件設定、プレビュー、スタイル管理を担当する。

## 入口

- `src/hydrology_graphs/launcher_entry.py`
  - アプリ選択ランチャーから開く入口。
- `src/hydrology_graphs/__init__.py`
  - パッケージの入口。

## 責務

- グラフ作成条件の入力
- スタイル調整
- プレビュー表示
- バッチ実行
- parquet 入力のスキャンと選択
- しきい値・スタイルの保存と読込

## 主な層

- `ui/`
  - 実行タブ、スタイルタブ、プレビュー、イベント処理、ビュー模型を置く。
- `services/`
  - UI からの要求をビジネスルールへ変換する。
- `domain/`
  - グラフ種別、時刻窓、集計ルール、定数、モデルを置く。
- `render/`
  - 実際の描画処理を置く。
- `io/`
  - parquet、style、threshold、PNG 出力の I/O を置く。

## データの流れ

1. UI が対象データ、期間、グラフ種別、スタイルを受け取る。
2. `services/usecases.py` がスキャン・検証・プレビュー・バッチの順を制御する。
3. `domain/logic.py` が時刻窓や判定ルールを決める。
4. `render/plotter.py` がグラフを描画する。
5. `io/` が parquet 読込、スタイル保存、PNG 保存を担当する。

## 補足

- この package は「条件設定・実行タブ」の本体。
- `style` と `threshold` は保存形式が別で、UI から更新する。
- グラフ作成前のスキャンや確認処理もここに含まれる。
