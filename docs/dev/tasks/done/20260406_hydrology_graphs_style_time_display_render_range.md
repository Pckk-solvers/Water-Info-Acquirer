# 表示モードに応じた描画レンジと表記切替

## 目的
- `datetime` と `1時~24時` で、X 軸ラベルだけでなく描画対象の時刻範囲も切り替える。
- 24時相当の表示を自然に見せるため、イベント系の capture range を表示モードに合わせて扱う。
- 24時表記の X 軸ラベルは時だけを表示し、分は出さない。

## 対象ファイル
- `src/hydrology_graphs/render/plotter.py`
- `tests/hydrology_graphs/test_plotter.py`

## 実施内容
- 24時表記時の X 軸フォーマットを時のみの表示にした。
- 表示モードに応じて、描画に使う時刻範囲を切り替えるようにした。
- `datetime` は通常の `00:00` 境界を使い、`1時~24時` は 24 時が自然に見える範囲を使う。
- X 軸ラベルの見え方を変えても、既存の非イベント系描画を壊さないようにした。

## 完了条件
- 表示モードで描画対象レンジが切り替わる。
- 24時相当が前日 `24` として見える。
- `datetime` 表記では通常の `00:00` 境界を使う。

## 確認方法
- `uv run pytest -q tests/hydrology_graphs/test_plotter.py tests/hydrology_graphs/test_services.py` を実行し、通過を確認した。

## 関連要件 / 関連設計
- 要件: `docs/dev/requirements/hydrology-graphs-platform.md`
- 設計: `docs/dev/architecture/hydrology_graphs.md`

## 結果
- 24時表記のラベルを `hh` 表示へ変更した。
- 表示モードに応じた描画レンジ切替の前提を整えた。
- `uv run pytest -q tests/hydrology_graphs/test_plotter.py tests/hydrology_graphs/test_services.py` を含む関連テストで通過を確認した。

## 残課題
- なし。
