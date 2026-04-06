# 起動経路


## 1. 直下 `main.py`

- リポジトリ直下の `main.py` は、アプリ本体ではなくランチャー起動用の薄いラッパー。
- `--dev` がある場合は `developer_mode=True` をランチャーへ渡す。
- 実際の処理本体は `src/water_info_acquirer/launcher.py` にある。

起動の流れ:

1. `main.py` が `argparse` で `--dev` を読む
2. `src.water_info_acquirer.launcher.main(developer_mode=...)` を呼ぶ
3. ランチャーが Tk root を作る
4. アプリカードから各アプリを選択する

## 2. ランチャー本体

- `src/water_info_acquirer/launcher.py`
- 1つの Tk root でアプリ選択画面を表示する。
- 選択後は `app_registry.py` の定義に従って各アプリの `launcher_entry` を開く。

## 3. アプリ別の直接起動

- `python -m water_info`
  - Water Info の GUI/CLI 起動入口。
- `python -m jma_rainfall_pipeline`
  - JMA 雨量データ取得の GUI/CLI 起動入口。
- `python river_rainfall.py`
  - 雨量整理・抽出の直接起動入口。
- `python main.py`
  - アプリ選択ランチャー起動入口。

## 4. 実態として欠けていないもの

- 直下 `main.py` は存在する。
- ただし `main.py` にアプリ実装は置かない。
- 画面ごとの実処理は各パッケージの `launcher_entry.py` と GUI/CLI に分離する。

## 5. 関連実装

- `main.py`
- `src/water_info_acquirer/launcher.py`
- `src/water_info_acquirer/app_registry.py`
- `src/water_info_acquirer/navigation.py`
- `src/water_info_acquirer/runtime.py`

