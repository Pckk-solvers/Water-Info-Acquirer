# Hydrology Graphs GUI 起動不能修正

## 目的
- Hydrology Graphs の GUI 起動時に発生していた `event_window_terminal_padding` 未定義エラーを解消する。
- 条件設定・実行タブとプレビュー/実行処理で参照する状態変数を整合させる。

## 対象ファイル
- `src/hydrology_graphs/ui/app.py`
- `src/hydrology_graphs/ui/tabs_execute.py`
- `src/hydrology_graphs/ui/execute_actions.py`
- `src/hydrology_graphs/ui/preview_actions.py`

## 実施内容
- `HydrologyGraphsApp` 初期化時に `event_window_terminal_padding` を `BooleanVar` として定義する。
- 既存の条件設定・実行タブで参照しているイベント窓補正チェックボックスと state を一致させる。
- GUI 初期化時に `tabs_execute.py` で参照される属性が揃うようにする。

## 完了条件
- GUI を起動したときに属性未定義で落ちない。
- 条件設定・実行タブの描画が最後まで完了する。
- 既存のプレビュー/実行処理が同じ state を参照できる。

## 確認方法
- `uv run python -c "import tkinter as tk; from hydrology_graphs.ui.app import show_hydrology_graphs; root=tk.Tk(); root.withdraw(); app=show_hydrology_graphs(parent=root); print('ok', type(app).__name__); app.destroy(); root.destroy()"`
- `uv run pytest -q tests/hydrology_graphs`

## 関連要件 / 関連設計
- 要件: `docs/dev/requirements/hydrology-graphs-platform.md`
- 設計: `docs/dev/architecture/hydrology_graphs.md`

## 結果
- `event_window_terminal_padding` の未定義による GUI 起動失敗を解消した。
- GUI 初期化確認コマンドで `ok HydrologyGraphsApp` を確認した。
- `uv run pytest -q tests/hydrology_graphs` で `41 passed` を確認した。

## 残課題
- なし。
