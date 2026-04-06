# water_info_acquirer architecture

`src/water_info_acquirer/` は、各アプリを束ねるトップレベルのランチャーを担当する。

## 入口

- `main.py`（リポジトリ直下）
  - アプリ選択ランチャーの薄い起動ラッパー。
- `src/water_info_acquirer/launcher.py`
  - ランチャー本体。
- `src/water_info_acquirer/app_registry.py`
  - 起動可能アプリの一覧と起動関数を管理する。
- `src/water_info_acquirer/navigation.py`
  - アプリ間の遷移メニューを作る。
- `src/water_info_acquirer/runtime.py`
  - frozen/dev のパス調整を行う。

## 責務

- アプリ選択画面の表示
- 各アプリの起動
- ヘルプリンクやカードの定義
- 開発モードの切り替え

## 配下の関係

- `app_meta.py`
  - アプリ名や表示情報の定義。
- `app_registry.py`
  - `water_info` / `jma_rainfall_pipeline` / `river_meta.rainfall` / `hydrology_graphs` の起動先を束ねる。
- `launcher.py`
  - Tk root を作り、カード選択と起動を実施する。
- `navigation.py`
  - 各アプリ内のメニュー遷移を支える。
- `runtime.py`
  - 実行環境ごとの import 解決を支える。

## データの流れ

1. 直下 `main.py` が `--dev` を読む。
2. `water_info_acquirer.launcher.main()` が起動する。
3. `app_registry.py` の定義に従って各アプリの `launcher_entry` を開く。
4. 遷移や終了制御は `navigation.py` と `runtime.py` が支える。

## 補足

- この package はドメインロジックを持たない。
- 実処理は各アプリ側 package に委ねる。
