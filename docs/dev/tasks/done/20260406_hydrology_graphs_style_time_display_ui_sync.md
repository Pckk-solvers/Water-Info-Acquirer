# スタイル調整タブの表示モード UI 同期

## 目的
- 表示モードの選択 UI をスタイル調整タブの左側へ寄せ、スタイル設定の一部として見えるようにする。
- UI の選択値と style JSON の `display.time_display_mode` を相互同期する。

## 対象ファイル
- `src/hydrology_graphs/ui/app.py`
- `src/hydrology_graphs/ui/tabs_style.py`
- `tests/hydrology_graphs/test_ui_support.py`

## 実施内容
- 表示モードの初期値を style JSON から反映するようにした。
- UI からの変更を style payload に反映し、保存時に保持されるようにした。
- 表示モードの操作部品をスタイル調整タブ左側へ配置した。
- 候補値や既定値が更新されたときのフォーム同期を壊さないようにした。

## 完了条件
- 表示モードのラジオボタンがスタイル設定領域内にある。
- UI の選択値が style JSON の値と一致する。
- 読込済み style JSON の表示モードが UI に復元される。

## 確認方法
- `uv run pytest -q tests/hydrology_graphs/test_ui_support.py` を実行し、通過を確認した。

## 関連要件 / 関連設計
- 要件: `docs/dev/requirements/hydrology-graphs-platform.md`
- 設計: `docs/dev/architecture/hydrology_graphs.md`

## 結果
- 表示モード枠を右ペインから左ペインへ移動した。
- `app.time_display_mode` を style payload の `display.time_display_mode` と同期するようにした。
- `uv run pytest -q tests/hydrology_graphs/test_ui_support.py` を含む関連テストで通過を確認した。

## 残課題
- なし。
