# アーキテクチャ概要

## 全体像

- `main.py` から `src/launcher.py` を起動し、ランチャーで各アプリ（Toplevel）を切り替えます。
- 子ウィンドウ側の「メニュー」から相互遷移できます。

## 主なコンポーネント

- ランチャー: `src/launcher.py`
- 画面名/バージョン: `src/app_names.py`
- 国交省 水文データ取得（GUI/取得/出力）: `src/water_info/`
- 気象庁 雨量データ取得（GUI）: `src/jma_rainfall_pipeline/gui/app.py`
- 気象庁 出力/ログ/パス解決: `src/jma_rainfall_pipeline/utils/config_loader.py`, `src/jma_rainfall_pipeline/utils/path_utils.py`, `src/jma_rainfall_pipeline/logger/app_logger.py`
- 雨量共通処理（river_meta）: `src/river_meta/rainfall/`

## rainfall パッケージの現構成

- 入口: `src/river_meta/rainfall/entry.py`, `src/river_meta/rainfall/cli.py`, `src/river_meta/rainfall/__main__.py`
- GUI: `src/river_meta/rainfall/gui/`
- ユースケース: `src/river_meta/rainfall/services/`
- ソース連携: `src/river_meta/rainfall/sources/`
- ストレージ: `src/river_meta/rainfall/storage/`
- 出力: `src/river_meta/rainfall/outputs/`
- ドメイン: `src/river_meta/rainfall/domain/`
- 共通補助: `src/river_meta/rainfall/support/`
