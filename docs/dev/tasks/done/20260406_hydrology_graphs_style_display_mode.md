# Hydrology Graphs 表示モード切替

## 目的
- スタイル調整タブで、24時表記と datetime 表記を選択できるようにする。
- 選択された表示モードに応じて、プレビューとサンプル出力の時刻表記を切り替える。
- 入力 Parquet の契約は維持しつつ、画面表示と描画表現だけを切り替える。

## 対象ファイル
- `docs/dev/requirements/hydrology-graphs-platform.md`
- `docs/dev/architecture/hydrology_graphs.md`
- `src/hydrology_graphs/ui/app.py`
- `src/hydrology_graphs/ui/tabs_style.py`
- `src/hydrology_graphs/ui/preview_actions.py`
- `src/hydrology_graphs/services/dto.py`
- `src/hydrology_graphs/services/usecases.py`
- `src/hydrology_graphs/render/plotter.py`

## 実施内容
- スタイル調整タブに表示モードの選択 UI を追加する。
- preview / sample output に表示モードを渡す。
- 選択モードに応じて、24時表記または datetime 表記へ切り替える。
- Parquet 側の datetime 正規化済み契約は維持する。

## 完了条件
- 24時表記 / datetime 表記を UI で選べる。
- プレビューとサンプル出力で選択した表示モードが反映される。
- 保存済み Parquet の契約や event window 処理を壊さない。

## 確認方法
- 同一データで、表示モード切替前後のプレビュー差分を確認する。
- 24時相当を含むデータで、表記切替が想定どおりに動くことを確認する。
- イベント系の窓補正と併用したときも、描画対象が崩れないことを確認する。

## 関連要件 / 関連設計
- 要件: `docs/dev/requirements/hydrology-graphs-platform.md`
- 設計: `docs/dev/architecture/hydrology_graphs.md`

## 結果
- スタイル調整タブに `1時~24時` / `datetime` の表示モード切替を追加した。
- preview と sample output で選択モードに応じて X 軸の時刻表記を切り替えるようにした。
- 24時相当のラベルは前日 `24:00` として表示するようにした。
- `uv run pytest -q` で `187 passed` を確認した。

## 残課題
- なし。
