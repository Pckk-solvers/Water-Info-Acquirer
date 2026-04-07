# time_display_mode 連動の切り出し窓切替

## 目的
- `24時表記` と `datetime表記` で、イベント系の切り出し窓を切り替える。
- `precheck / preview / batch` で同じ判定を使い、READY と描画結果の不整合を防ぐ。

## 対象ファイル
- `src/hydrology_graphs/services/dto.py`
- `src/hydrology_graphs/services/usecases.py`
- `src/hydrology_graphs/ui/execute_actions.py`
- `src/hydrology_graphs/ui/preview_actions.py`
- `tests/hydrology_graphs/test_preview_actions.py`
- `tests/hydrology_graphs/test_services.py`

## 実施内容
- `time_display_mode` から `event_window_terminal_padding` を決める処理を追加した。
- `run_precheck` は現在の表示モードで padding 有無を決めるようにした。
- `preview` は style payload の表示モードで padding 有無を決めるようにした。
- `batch` は style payload の表示モードをサービスへ渡し、描画と切り出しを一致させるようにした。

## 完了結果
- `24時表記` では終端 +1h で評価・描画されるようになった。
- `datetime表記` では終端 +1h を使わず評価・描画されるようになった。
- `precheck / preview / batch` で窓判定が揃った。

## 確認方法
- `uv run pytest -q tests/hydrology_graphs/test_preview_actions.py tests/hydrology_graphs/test_services.py`
