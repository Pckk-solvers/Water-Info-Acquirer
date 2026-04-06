# スタイル表示モードの JSON 同期と配置変更

## 目的
- スタイル調整タブの時刻表示モードを style JSON に保存し、読み込み時に復元できるようにする。
- 表示モードの GUI をスタイル調整タブの左側へ移し、スタイル設定としてまとまって見えるようにする。
- 既存の `datetime` 既定を維持しつつ、プレビューとサンプル出力へ同じ設定を渡し、表示モードに応じてデータ範囲も切り替える。

## 対象ファイル
- `docs/dev/requirements/hydrology-graphs-platform.md`
- `docs/dev/architecture/hydrology_graphs.md`
- `src/hydrology_graphs/io/style_store.py`
- `src/hydrology_graphs/ui/app.py`
- `src/hydrology_graphs/ui/tabs_style.py`
- `src/hydrology_graphs/ui/preview_actions.py`
- `src/hydrology_graphs/render/plotter.py`
- `tests/hydrology_graphs/test_ui_support.py`

## 実施内容
- style JSON に `display.time_display_mode` を追加する。
- JSON 読込・保存・フォーム同期を対応させる。
- 表示モードの操作部品をスタイル調整タブ左側へ移動する。
- プレビューとサンプル出力が JSON 上の設定を参照するようにする。
- `datetime` は通常の `00:00` 境界、`1時~24時` は 24:00 が自然に見える範囲を使う。

## 完了条件
- 表示モードが style JSON に保存される。
- 読込時に保存済みの表示モードが復元される。
- GUI の配置がスタイル設定領域へ移る。
- 表示モードに応じて、プレビューとサンプル出力の範囲が変わる。
- 既存スタイル JSON と互換性がある。

## 確認方法
- style JSON の保存/読込で表示モードが維持されることを確認する。
- 24時表記 / datetime 表記の切替がプレビューに反映されることを確認する。
- 既存 JSON を読み込んだときに `datetime` 既定で動くことを確認する。
- 表示モード切替で、X軸ラベルだけでなく描画対象の時刻範囲も切り替わることを確認する。

## 関連要件 / 関連設計
- 要件: `docs/dev/requirements/hydrology-graphs-platform.md`
- 設計: `docs/dev/architecture/hydrology_graphs.md`
