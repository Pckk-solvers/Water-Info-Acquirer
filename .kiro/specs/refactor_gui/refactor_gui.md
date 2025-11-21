## GUI再設計（単一Tkルート＋Toplevel子ウィンドウ）計画

### 現状
- ランチャー（src/launcher.py）が自前で Tk を作り、子アプリを開く前に destroy。
- water_info / jma_rainfall_pipeline は各々 Tk を生成し mainloop する実装。
- Tk が複数になるため安全ではなく、戻りやアプリ間遷移がやりにくい。

### 目標
- プロセス内の Tk は1つ（ランチャーのルート）。
- 子画面は Toplevel（または Frame）として生成し、閉じたらランチャーに戻せる。
- UX：ランチャーで water/jma を選択→子画面表示→閉じたらランチャー再表示。後で相互遷移（子から子）も可能な構造に。
- PyInstaller 配布でも動くようパス/カレントの取り扱いを維持。

### 設計方針
1) ランチャーを唯一のルートにする  
   - ランチャー Tk は保持。子を開くときはランチャーを withdraw、子が閉じたら deiconify してボタンを再有効化。  
   - cwd/path 変更は既存実装を継続（凍結/非凍結で src を sys.path に追加）。

2) 子ウィンドウ側のエントリを抽象化  
   - 各アプリに「親Tkを受け取って Toplevel を返す/表示する」関数を用意。  
     例) water_info: `show_water(parent, on_close=None)`  
         jma:        `show_jma(parent, on_close=None)` （新設）  
   - 既存の CLI/単独起動は、上記 builder を呼ぶための薄いラッパ（新規 Tk を作って mainloop）を保持し、`python -m src.water_info` などは従来どおり動くようにする。

3) 閉じる→戻るの挙動  
   - 子Toplevelの WM_DELETE_WINDOW をフックし、destroy のあと on_close を呼ぶ。on_close でランチャー復帰（deiconify）とボタン有効化。

4) 依存ロード  
   - water_info の遅延インポートは維持。  
   - jma 側の重い import を必要箇所に寄せるか、ランチャーのバックグラウンドプリロードで吸収（現行プリロード処理をToplevel構造でも活かす）。

5) パス対応（凍結含む）  
   - ランチャーは実行ファイル親を PROJECT_ROOT に設定済み。子もそれを前提に動く。  
   - jma の get_project_root が __file__ を基準にしているため、凍結時は sys.executable 親を返す分岐を別タスクで検討。

### 作業ステップ
1) jma側改修  
   - Tk 生成と mainloop を分離し、`show_jma(parent, on_close)` のようなビルダーを実装。  
   - 子ウィンドウクローズ時に on_close を呼ぶ。CLI用 main()/run() は新規 Tk を作ってビルダーを呼ぶ形に変更。

2) water_info改修  
   - 同様に Tk 生成と mainloop を分離。`show_water(parent, on_close)` を用意し、既存 main() は薄いラッパに。  
   - WWRApp が root 前提でないか確認し、必要なら master 指定を追加。

3) ランチャー改修  
   - 単一 root を保持し、ボタンで子を表示。子実行中はボタン無効化＆ランチャー withdraw。子終了で再表示。  
   - 既存の依存プリロードスレッドは継続。

4) 確認  
   - `uv run python -m src.launcher` で双方の起動/戻りを確認。  
   - 必要なら PyInstaller onedir ビルドで凍結時のパス/出力先を確認（config.yml 取り扱い含む）。

### リスク・留意点
- Tkを子に渡す際、各モジュール内で master 未指定のウィジェットがあると親に紐づかない可能性。master 引数の追加が必要になる場合がある。  
- 子内部で threading を使う処理がある場合、ウィンドウ破棄後の例外処理を確認。  
- jma の出力パス解決は別途 get_project_root の凍結対応が必要になるかもしれない。
