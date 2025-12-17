# アーキテクチャ概要

## 全体像

- `main.py` から `src/launcher.py` を起動し、ランチャーで各アプリ（Toplevel）を切り替えます。
- 子ウィンドウ側の「メニュー」から相互遷移できます。

## 主なコンポーネント

- ランチャー: `src/launcher.py`
- 画面名/バージョン: `src/app_names.py`
- 国交省 水文データ取得（GUI/取得/出力）: `src/water_info/main_datetime.py`
- 気象庁 雨量データ取得（GUI）: `src/jma_rainfall_pipeline/gui/app.py`
- 気象庁 出力/ログ/パス解決: `src/jma_rainfall_pipeline/utils/config_loader.py`, `src/jma_rainfall_pipeline/utils/path_utils.py`, `src/jma_rainfall_pipeline/logger/app_logger.py`

