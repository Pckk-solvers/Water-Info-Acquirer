
## 要件定義: 単一Tkルート＋子ウィンドウ相互起動（Toplevel前提）

### 背景と目的
- 現状はランチャー・water_info・jma_rainfall_pipeline がそれぞれ Tk を生成し mainloop で動作。複数の Tk 同居は不安定で、戻りやアプリ間遷移が扱いづらい。
- 目標は「プロセス内の Tk を1つに統一し、子画面は Toplevel（または Frame）で運用」して UX と安定性を高める。
- ランチャーは最初の入口のみで利用し、以後は子ウィンドウ間を直接行き来できるようにしてユーザー操作を減らす。
- PyInstaller 配布（凍結）でも同じ起動導線を維持する。

### 機能要件
1. プロセス内の Tk は1つ（ランチャーのルート）。子画面は Toplevel で生成・表示（Frame切替は採用しない）。
2. ランチャーから water_info / jma を初回だけ開けることに加え、各子画面から他方の子画面を直接開けること（ランチャーを再表示せずに遷移）。
3. water_info / jma それぞれが「親Tkを受け取り Toplevel を生成する」エントリを提供すること（例: `show_water(parent, on_open_other=None)`, `show_jma(parent, on_open_other=None)`）。`on_open_other` で相互遷移をトリガー可能。
4. 子画面を閉じたらアプリ全体を終了すること（ルートだけ残さない）。
5. 従来の CLI / 単体起動も維持するが、PyInstaller では `main.py` からランチャーを直接呼ぶ方針とし、`python -m` には依存しない。
6. ランチャー起動時の依存プリロードを継続・適用し、子起動前に依存欠けを検知できること。
7. 凍結/非凍結どちらでも、必要なモジュールと config.yml を正しく参照できること。

### 非機能要件
- 安定性: Tk インスタンスはプロセス内で常に1個。子は Toplevel 化し、破棄順序でクラッシュしないようにする。
- UX: ランチャー表示はできるだけ早く、子起動・相互遷移の体感を悪化させない（プリロードや非同期処理は現状踏襲）。
- 配布: PyInstaller onedir/onefile でも起動導線が変わらないようにする。必要に応じて `--add-data` 等で config.yml を同梱。
- ログ/パス: 出力パスやログファイルが凍結時にも意図通りの場所に作成されるようにする（path_utils の凍結対応を jma/water_info 両方に入れる）。

### 変更対象と前提
- ランチャー (`src/launcher.py`): 単一 Tk を保持し、子を Toplevel で開く。ボタン有効/無効切替と withdraw/deiconify 制御を維持。必要に応じて「他方を開く」イベントを受け取れるようにする。
- water_info: Tk 生成+mainloop とロジックを分離し、親Tk渡しのビルダー関数を新設。既存 main() はラッパとして維持し、相互遷移用の on_open_other コールバックを受け付ける。
- jma_rainfall_pipeline: 同様に Tk 生成+mainloop を分離。`sys.exit` 前提の main はラッパ化し、ビルダーで Toplevel を生成。on_open_other コールバック対応。
- 依存プリロード: 現行の依存チェック（import）をランチャー側で継続。子のビルダーでも必要に応じて遅延 import を活用。

### 検討事項・リスク
- マスター未指定のウィジェットがある場合、親Tkに紐付かない恐れがあるため、必要に応じて master 指定を追加。
- jma の path_utils が __file__ 基準のため、凍結時に意図しないディレクトリを指す可能性。別タスクで修正を検討。
- 子画面内のスレッド処理がウィンドウ破棄後に例外を吐かないか確認が必要。
- PyInstaller onefile では初回展開が遅い場合があるため、起動体感を悪化させない工夫（プリロードのタイミングなど）を考慮。

### 受け入れ条件（例）
- `uv run python -m src.launcher` でランチャーが起動し、water_info/jma 子画面が開閉できること（Tk は1個のまま）。
- 子画面内から「他方の子画面を開く」操作ができ、ランチャーを経由せず遷移できること。
- `uv run python -m src.water_info` / `uv run python -m src.jma_rainfall_pipeline` で単独起動が従来どおり動き、Tk が二重生成されないこと。
- （任意）PyInstaller onedir ビルドで同様の動作を確認し、設定ファイル/出力ディレクトリが意図通りに解決されること。
