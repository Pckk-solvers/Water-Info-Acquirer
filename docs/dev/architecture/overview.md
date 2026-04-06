# アーキテクチャ概要


## 全体像

- リポジトリ直下の `main.py` は薄い起動ラッパーで、`src.water_info_acquirer.launcher.main(developer_mode=...)` を呼び出す。
- `src/water_info_acquirer/launcher.py` が本体のアプリ選択ランチャーで、Tk root を 1 つ生成し、各アプリの Toplevel を切り替える。
- 子ウィンドウ側の「メニュー」から相互遷移できる。

## 主なコンポーネント

- ランチャー: `src/water_info_acquirer/launcher.py`
- 画面名/バージョン: `src/water_info_acquirer/app_meta.py`
- 国交省 水文データ取得（GUI/取得/出力）: `src/water_info/`
- 気象庁 雨量データ取得（GUI）: `src/jma_rainfall_pipeline/gui/app.py`
- 気象庁 出力/ログ/パス解決: `src/jma_rainfall_pipeline/utils/config_loader.py`, `src/jma_rainfall_pipeline/utils/path_utils.py`, `src/jma_rainfall_pipeline/logger/app_logger.py`
- 雨量共通処理（river_meta）: `src/river_meta/rainfall/`

## 入口の分担

- 直下 `main.py`: アプリ選択ランチャーのエントリ。`--dev` で開発者モードを渡す。
- `src/water_info_acquirer/launcher.py`: アプリカード表示、選択、相互遷移、ヘルプ表示を担当。
- `src/water_info_acquirer/app_registry.py`: 起動可能アプリ一覧と起動関数を管理する。
- `src/water_info_acquirer/navigation.py`: 各アプリのメニュー遷移を構築する。
- `src/water_info_acquirer/runtime.py`: frozen/dev の両方で `src` をパスに通す。
- `src/jma_rainfall_pipeline/__main__.py`: JMA の GUI/CLI 起動入口。
- `src/water_info/__main__.py`: Water Info の GUI/CLI 起動入口。
- `src/river_meta/rainfall/__main__.py`: 雨量整理・抽出の GUI/CLI 起動入口。
- `river_rainfall.py`: 既存の rainfall 直接起動ラッパー。

## rainfall パッケージの現構成

- 入口: `src/river_meta/rainfall/entry.py`, `src/river_meta/rainfall/cli.py`, `src/river_meta/rainfall/__main__.py`
- GUI: `src/river_meta/rainfall/gui/`
- ユースケース: `src/river_meta/rainfall/services/`
- ソース連携: `src/river_meta/rainfall/sources/`
- ストレージ: `src/river_meta/rainfall/storage/`
- 出力: `src/river_meta/rainfall/outputs/`
- ドメイン: `src/river_meta/rainfall/domain/`
- 共通補助: `src/river_meta/rainfall/support/`

