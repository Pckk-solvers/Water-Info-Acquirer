# 観測所一覧のメトリクス候補をファイル名から推定

## 目的
- 条件設定・実行タブの観測所一覧で、`雨量 / 流量 / 水位` のメトリクス候補を軽量スキャン段階から見えるようにする。
- 気象庁は雨量固定として扱う。
- 軽量スキャンは重くしすぎず、詳細検証は従来どおり列ベースの `metric` を正とする。

## 対象ファイル
- `docs/dev/requirements/hydrology-graphs-platform.md`
- `docs/dev/architecture/hydrology_graphs.md`
- `src/hydrology_graphs/io/parquet_store.py`
- `src/hydrology_graphs/ui/app.py`
- `src/hydrology_graphs/ui/view_models.py`
- `tests/hydrology_graphs/test_parquet_store.py`
- `tests/hydrology_graphs/test_ui_support.py`

## 実施内容
- 軽量スキャンでファイル名からメトリクス候補を推定する処理を追加した。
- 気象庁は雨量固定として扱うようにした。
- 観測所一覧のチェック行で `source_label:station_key (station_name) / 雨量 / 流量 / 水位` を表示できるようにした。

## 完了結果
- 軽量スキャンの観測所一覧にメトリクス候補が表示されるようになった。
- `jma` は `雨量` 固定、`water_info` はファイル名由来の候補を集約して表示するようになった。
- フルスキャンの列ベース判定は維持している。

## 確認方法
- `uv run pytest -q tests/hydrology_graphs/test_parquet_store.py tests/hydrology_graphs/test_ui_support.py`
- `uv run pytest -q`

## 関連要件 / 関連設計
- 要件: `docs/dev/requirements/hydrology-graphs-platform.md`
- 設計: `docs/dev/architecture/hydrology_graphs.md`
