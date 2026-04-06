# 開発（保守・改修・ビルド）

このセクションは、改修や配布（PyInstaller）を行う方向けの情報です。

- 一般文書:
  - `docs/dev/architecture.md`
  - `docs/dev/build-pyinstaller.md`
  - `docs/dev/github-pages.md`
  - `docs/dev/release-process.md`
- ランチャー: `src/launcher.py`
- 国交省 水文データ取得: `src/water_info/`
- 気象庁 雨量データ取得: `src/jma_rainfall_pipeline/`
- 共通雨量 GUI/CLI/サービス: `src/river_meta/rainfall/`
  - 入口: `entry.py`, `cli.py`, `__main__.py`
  - GUI: `gui/`
  - ユースケース: `services/`
  - source連携: `sources/`
  - ストレージ: `storage/`
  - 出力: `outputs/`
  - ドメイン/共通ロジック: `domain/`, `support/`
- Hydrology Graphs Platform: `docs/dev/hydrology-graphs-platform/README.md`
  - current: `docs/dev/hydrology-graphs-platform/current/README.md`
  - current spec: `docs/dev/hydrology-graphs-platform/current/spec.md`
  - current style contract: `docs/dev/hydrology-graphs-platform/current/style-contract.md`
  - current parquet contract: `docs/dev/hydrology-graphs-platform/current/parquet-contract.md`
  - current threshold contract: `docs/dev/hydrology-graphs-platform/current/threshold-contract.md`
- Water Info 後処理仕様: `docs/dev/postprocess/specification.md`
- 旧計画メモや差分案は削除済み
