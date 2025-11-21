## タスク分解（動作確認前提）

### 0. 方針/入口確認
- [ ] ランチャーは初回のみ表示。子は Toplevel 相互遷移。`main.py` を PyInstaller 入口にし、`-m` 依存なしで動くルートを設計に反映。

### 1. jma_rainfall_pipeline（優先高）
- [ ] Tk生成+mainloop を分離し、`show_jma(parent, on_open_other=None)` を新設（Toplevel生成）。
- [ ] `main()`/`run()` をラッパ化し、ランチャー呼び出し時に sys.exit を踏まない入口を用意。
- [ ] master/親紐付けを確認・修正。
- [ ] path_utils.get_project_root を凍結時は `Path(sys.executable).parent` に分岐。config.yml の参照パスも確認。
- 動作確認: `uv run python -m src.jma_rainfall_pipeline`（単独起動で Tk 二重なし）。

### 2. water_info（優先中）
- [ ] Tk生成+mainloop を分離し、`show_water(parent, on_open_other=None)` を新設（Toplevel生成）。
- [ ] `__main__.py` / main() をラッパ化して非凍結での `-m` 互換を維持。
- [ ] master/親紐付けを確認・修正。
- [ ] 凍結時ルート解決が必要なら path_utils 相当を整備（jmaと揃える）。
- 動作確認: `uv run python -m src.water_info`（単独起動で Tk 二重なし）。

### 3. ランチャー統合（優先中）
- [ ] Tk を一度だけ生成し、ボタンで `show_water` / `show_jma` を起動。初回のみランチャーUIを出す。
- [ ] 依存プリロード（importlib）を継続し、完了後ボタン有効化。
- [ ] on_open_other を受け、現在の子Toplevelを閉じて他方を開く制御を実装。
- [ ] 子が閉じたら親Tk.destroy() で全体終了する挙動を確認。
- 動作確認: `uv run python -m src.launcher` → water → jma → 閉じる（逆パスも）で一連を確認。

### 4. 凍結対応/追加確認（時間に応じて）
- [ ] PyInstaller onedir でビルドし、config.yml 同梱確認。
- [ ] 凍結時に path_utils 分岐が効いて出力パス/ログパスが意図通りかを軽く確認。
