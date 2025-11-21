## 設計: 単一Tkルート＋Toplevel子相互遷移

### 全体アーキテクチャ
- Tkインスタンスは1つ（ランチャーが保持）。ランチャーは初回のみ表示。
- 子画面（water_info / jma）は親Tk配下の Toplevel として生成・表示。Frame切替は行わない。
- 子→子遷移はコールバック経由で実行し、ランチャーを経由しない。
- 子を閉じたらアプリ全体を終了（親Tk破棄）。

### エントリーポイント
- `main.py`：ランチャーを起動（PyInstallerでもここを入口とする）。
- `src/launcher.py`：
  - 唯一の Tk を生成。
  - 初回だけ水文/JMAいずれかの子を開くボタンを表示（最小UI）。
  - 依存プリロードをバックグラウンドで実行（現行の依存チェックを継続）。
  - ボタン押下で親Tkを保持したまま子 Toplevel を生成し、ランチャーのボタンは無効化 or ランチャーは非表示でも可（設計時に選択）。
- `src/water_info/...` / `src/jma_rainfall_pipeline/...`：
  - それぞれ `show_water(parent: Tk, on_open_other=None)` / `show_jma(parent: Tk, on_open_other=None)` を提供。
  - 従来の `main()` / CLI は新規 Tk を作って上記 `show_*` を呼ぶ薄いラッパとして維持（非凍結環境向け互換）。

### 画面ライフサイクル
1) ランチャー起動（Tk生成・依存プリロード開始）。
2) ユーザーが water/jma ボタンを押す → ランチャーUIは隠す/無効化。
3) 対応する `show_*` で Toplevel を生成し、起動。
4) 子内の「他方を開く」ボタンで `on_open_other("water"|"jma")` を呼ぶ。
   - 現在の子Toplevelを破棄 → 他方の `show_*` を呼んで新規Toplevel表示。
5) 子Toplevelの閉じる（×）/終了操作で親Tkを destroy → アプリ終了。

### コールバック設計
- `on_open_other`: 子→子遷移を要求するコールバック。ランチャー（またはコントローラ）が受け取り、現在の子Toplevelを閉じて他方を開く。
- `on_close`: 今回は「閉じたら終了」方針のため不要。Toplevel WM_DELETE_WINDOW で親Tk.destroy() を呼ぶ実装とする。

### モジュールごとの改修ポイント
- launcher
  - Tk生成は一度きり。
  - ボタン→ `show_water` / `show_jma` 呼び出し。on_open_other のハンドラを提供。
  - 依存プリロード（importlib）継続。
- water_info
  - Tk生成/ mainloop を分離し、Toplevel生成ファクトリ `show_water` を新設。
  - UI生成時は master を明示して親Tk/Toplevelに紐付ける。
  - 従来 main()/__main__ は新規 Tk を生成するラッパとして維持（非凍結向け）。
- jma_rainfall_pipeline
  - 同様に Tk生成/mainloop 分離、`show_jma` を新設。
  - `main()` の sys.exit(run()) パスはラッパ化し、ランチャーからは exit を踏まない入口を使う。

### パス/凍結対応
- `path_utils.get_project_root`（jma）を凍結時は `Path(sys.executable).parent` を返すよう分岐。
- water_info 側も同様に凍結時のルート解決が必要なら整備（共通ユーティリティ化を検討）。
- ランチャーは既に `sys.frozen` 判定で cwd/paths を設定済み。子もこの前提を利用。
- PyInstaller: onefile/onedir いずれでも `main.py` からランチャーを起動。`--add-data` で `config.yml` を含める。

### 非機能・UX
- 依存プリロードをランチャー初期化後すぐ開始し、子起動を軽くする。
- Toplevel間遷移は即時に行う（フェード等の演出は不要）。
- ランチャーUIは最小限（初回のみ使用）。

### テスト観点
- `uv run python -m src.launcher`: ランチャー表示→子起動→子内で他方を開く→閉じると終了。
- `uv run python -m src.water_info` / `uv run python -m src.jma_rainfall_pipeline`: 単独起動で Tk が重複しない。
- PyInstaller onedir ビルドで config パス・出力パスが意図通りか確認（path_utils 分岐の有無含む）。
