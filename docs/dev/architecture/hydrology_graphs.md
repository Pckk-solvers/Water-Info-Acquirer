# Hydrology Graphs architecture

`src/hydrology_graphs/` は、雨量・水位グラフの作成と条件設定、プレビュー、スタイル管理を担当する。

## 入口

- `src/hydrology_graphs/launcher_entry.py`
  - アプリ選択ランチャーから開く入口。
- `src/hydrology_graphs/__init__.py`
  - パッケージの入口。

## 責務

- グラフ作成条件の入力
- イベント系の基準日窓に対する終端余白の制御
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
2. UI がイベント系の窓補正設定を含めて条件を組み立てる。
3. `services/usecases.py` がスキャン・検証・プレビュー・バッチの順を制御する。
4. `domain/logic.py` が logical window（3日/5日）と、24時相当を保持するための capture window を決める。入力 parquet は `water_info` / `jma` の datetime 正規化済みデータを前提とする。
5. `render/plotter.py` が capture window のデータをもとにグラフを描画する。
6. `io/` が parquet 読込、スタイル保存、PNG 保存を担当する。

## 補足

- この package は「条件設定・実行タブ」の本体。
- `style` と `threshold` は保存形式が別で、UI から更新する。
- グラフ作成前のスキャンや確認処理もここに含まれる。
- イベント系は、基準日を中心とした論理窓と、24時相当を落とさないための終端余白付き窓を分けて扱う。
- 24時相当は source 側で翌日 `00:00:00` に正規化済みだが、描画前の窓端判定では終端余白が必要になる。
