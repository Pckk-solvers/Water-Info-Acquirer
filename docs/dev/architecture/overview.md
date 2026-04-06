# アーキテクチャ概要

この文書は `docs/dev/architecture/` のハブ。
個別の責務は package 単位の文書へ分ける。

## 起動の階層

1. リポジトリ直下の `main.py`
   - アプリ選択ランチャーを起動する薄いラッパー。
2. `src/water_info_acquirer/`
   - 複数アプリを束ねるトップレベルのランチャー。
3. 各アプリ package
   - `water_info`
   - `jma_rainfall_pipeline`
   - `river_meta/rainfall`
   - `hydrology_graphs`

## 主要 package の役割

- `docs/dev/architecture/water_info.md`
  - 水文水質データの取得・正規化・出力。
- `docs/dev/architecture/jma_rainfall_pipeline.md`
  - 気象庁雨量データの取得・解析・出力。
- `docs/dev/architecture/river_meta_rainfall.md`
  - JMA と Water Info の雨量データの集約・分析・出力。
- `docs/dev/architecture/hydrology_graphs.md`
  - グラフ条件設定、スタイル調整、プレビュー、バッチ実行。
- `docs/dev/architecture/water_info_acquirer.md`
  - アプリ選択ランチャーと相互遷移。

## 実行経路の正本

- `docs/dev/architecture/entrypoints.md`
  - `main.py` を含む直接起動経路の正本。

## 迷ったら見る順番

1. `entrypoints.md`
2. 該当 package の architecture 文書
3. `domain/` と `requirements/`

