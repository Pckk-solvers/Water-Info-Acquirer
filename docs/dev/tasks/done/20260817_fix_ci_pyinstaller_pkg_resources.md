# CIのPyInstallerスモークビルド修正

## 目的

- GitHub ActionsのWindows向けPyInstallerスモークビルドを成功させる。
- `setuptools`の脆弱性対応版を維持したまま、`pkg_resources`削除後の互換性問題を解消する。

## 対象ファイル

- `pyproject.toml`
- `uv.lock`
- `docs/dev/tasks/active/README.md`

## 実施内容

- `pkg_resources`を間接的に利用しないPyInstaller 6.17以降を要求するよう更新した。
- `uv.lock`を再生成し、PyInstaller 6.16.0から6.22.1へ、PyInstaller hooksを2025.9から2026.6へ更新した。
- CIと同じPyInstallerスモークビルドをローカルで確認した。

## 完了条件

- `setuptools>=83.0.0`を維持した状態でPyInstallerを起動できる。
- `uv run pyinstaller --noconfirm water_info_acquirer_onedir.spec`が成功する。
- GitHub ActionsのCIが成功する。

## 確認方法

- `uv lock --check`
- `uv run pyinstaller --noconfirm water_info_acquirer_onedir.spec`
- `uv run ruff check src tests`
- `uv run pytest tests/river_meta -q`
- `uv run mkdocs build -q`
- GitHub Actionsの再実行結果確認

## 結果

- `uv lock --check`が成功した。
- PyInstaller 6.22.1でWindows向けonedirスモークビルドが成功した。
- `uv run ruff check src tests`が成功した。
- `uv run pytest tests/river_meta -q`で94件すべて成功した。
- `uv run mkdocs build -q`が成功した。
- GitHub Actionsはプッシュ後に再確認する。

## 残課題

- なし。PyInstallerスモークビルドを含むGitHub Actionsの再実行結果をプッシュ後に確認する。

## 関連要件 / 関連設計

- 要件: 既存の配布用PyInstaller成果物を生成できること
- 設計: `docs/dev/reference/build-pyinstaller.md`

## 実装着手前の自己レビュー結果

- 観点1: 脆弱性対応済みの`setuptools`を古い版へ戻していないか
  - 判定: OK。PyInstaller側を更新して互換性を確保した。
- 観点2: CI失敗の対象範囲に限定した修正か
  - 判定: OK。アプリケーションコードと実行時依存は変更していない。
- 観点3: ローカル確認だけでなくGitHub Actionsの成功まで確認できるか
  - 判定: OK。プッシュ後のCI結果を確認する。
