# 位況・流況後処理GUIと動的ラベル

## 目的

- 取得済みの水文水質データをGUIから後処理できるようにする。
- 水位・流量の選択に応じて、画面とExcel出力のラベルを動的に切り替える。
- 新しいGUIを既存ランチャーへ独立アプリとして統合する。

## 対象ファイル

- `docs/dev/requirements/postprocess.md`
- `docs/dev/requirements/postprocess-gui.md`
- `docs/dev/architecture/postprocess_gui.md`
- `docs/dev/architecture/water_info.md`
- `docs/dev/architecture/water_info_acquirer.md`
- `docs/dev/adr/20260817_postprocess_gui_dynamic_metric_labels.md`
- `src/water_info/postprocess.py`
- `src/water_info/postprocess_labels.py`
- `src/water_info/postprocess_service.py`
- `src/water_info/ui/postprocess_app.py`
- `src/water_info/postprocess_launcher_entry.py`
- `src/water_info_acquirer/app_meta.py`
- `src/water_info_acquirer/app_registry.py`
- `water_info_acquirer_onedir.spec`
- `water_info_acquirer_onefile.spec`
- `water_info_flow_and_level_statistics_guide.md`
- `docs/advanced/postprocess.md`
- `docs/user/launcher.md`
- `mkdocs.yml`
- `tests/water_info/`

## 実施内容

1. 対象種別と動的ラベルの要件・設計を文書化する。
2. 水位/流量の表示ラベル定義を一元化する。
3. 後処理CLIに対象種別指定を追加し、Excel見出しを動的化する。
4. 入力・出力・対象種別を指定できるTkinter GUIを作成する。
5. 既存ランチャーとアプリ間メニューへ統合する。
6. ラベル定義、CLI、GUI実行境界をテストする。

## 完了条件

- 水位と流量の両方で、GUIの説明・ラベル例・Excel見出しが一致する。
- `_WH`/`_QH`のファイル選択時に対象種別の初期値候補が設定される。
- GUIから時間データ、任意の日データ、Excel/Parquet出力先を指定して後処理を実行できる。
- 既存ランチャーから後処理GUIを起動できる。
- 既存の取得GUI・CLI・テストに影響がない。

## 確認方法

- `uv run pytest tests/water_info -q`
- `uv run ruff check src tests`
- GUIモジュールのimport確認
- 表示ラベル定義の水位/流量比較テスト
- 既存ランチャーの登録内容確認

## 関連要件 / 関連設計

- 要件: `docs/dev/requirements/postprocess.md`
- 要件: `docs/dev/requirements/postprocess-gui.md`
- 設計: `docs/dev/architecture/postprocess_gui.md`
- ADR: `docs/dev/adr/20260817_postprocess_gui_dynamic_metric_labels.md`

## 実装着手前の自己レビュー結果

- 観点1: 取得処理と後処理処理を混在させないか
  - 判定: OK。後処理GUIは取得済みファイルだけを入力とする。
- 観点2: 水位固定の表示を個別置換で増やさないか
  - 判定: OK。対象種別定義とラベル生成を専用モジュールへ集約する。
- 観点3: 既存CLIの互換性を壊さないか
  - 判定: OK。`--metric`の省略時は水位を既定値にする。
- 観点4: GUI処理でTkイベントループをブロックしないか
  - 判定: OK。後処理はワーカースレッドで実行する。

## 実施結果

- 水位/流量の定義とExcel表示ラベルを`postprocess_labels.py`へ集約した。
- 後処理CLIへ`--metric water_level|discharge`を追加し、省略時は水位とした。
- 位況・流況後処理GUIを追加し、時間データ、任意の日データ、Excel/Parquet出力先を指定できるようにした。
- GUIの対象種別切り替え、`_WH`/`_QH`による初期値推定、バックグラウンド実行を実装した。
- 既存ランチャー、アプリ間メニュー、PyInstaller onedir/onefile設定へ統合した。
- リポジトリ直下の手順書、ユーザー向けCLI/ランチャー文書、開発者向け要件・設計・ADRを現行仕様へ同期した。

## 確認結果

- `uv run ruff check src tests`: 成功
- `uv run pytest tests/water_info -q`: 47 passed
- `uv run pytest -q`: 250 passed（既存警告13件）
- `uv run mkdocs build -q`: 成功
- GUIモジュールのimport、Tk画面生成、対象種別切り替え: 成功
- `uv run pyinstaller --noconfirm water_info_acquirer_onedir.spec`: 成功

## 残課題

- なし。実データでの業務上の算定結果確認は、利用者が取得した対象ファイルをGUIへ指定して行う。
