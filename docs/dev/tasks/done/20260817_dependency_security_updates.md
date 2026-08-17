# Python依存関係のセキュリティ更新

## 目的

- GitHub Dependabotが`uv.lock`に対して検出した既知脆弱性を解消する。
- 直接依存の指定、ロックファイル、pip代替インストール用の`requirements.txt`を整合させる。
- 実行時依存と開発・ビルド依存を区別し、不要な大規模アップデートを避ける。

## 対象ファイル

- `pyproject.toml`
- `requirements.txt`
- `uv.lock`
- `docs/dev/tasks/active/README.md`

## 実施内容

- GitHub警告の修正版下限へ直接依存の制約を更新した。
- `uv`で間接依存を含むロックファイルを更新した。
- `requirements.txt`の直接依存制約を`pyproject.toml`と合わせた。
- `Pillow`、`pymdown-extensions`、`soupsieve`、`idna`などの間接依存もロック上の安全版へ更新した。

## 完了条件

- `pyproject.toml`、`requirements.txt`、`uv.lock`の依存定義が矛盾しない。
- 既存の取得・後処理・GUI・グラフ生成に必要な依存関係が解決できる。
- 変更内容と確認結果がタスク記録に残る。

## 確認方法

- `uv lock --check`
- `uv run pytest -q`
- `uv run ruff check src tests`
- GitHub Dependabot alertsの再確認

## 結果

- 直接依存の下限を更新した。
  - `cryptography>=50.0.0`
  - `pyarrow>=23.0.1`
  - `urllib3>=2.7.0`
  - `pytest>=9.0.3`
  - `setuptools>=83.0.0`
- `uv.lock`を更新した。
  - `cryptography` 50.0.0
  - `Pillow` 12.3.0
  - `pymdown-extensions` 11.0.1
  - `soupsieve` 2.9.2
  - `urllib3` 2.7.0
  - `pyarrow` 25.0.1
  - `idna` 3.18
  - `pytest` 9.1.1
  - `setuptools` 84.0.0
- `uv lock --check`が成功した。
- `uv run pytest -q`で243件すべて成功した。
- `uv run ruff check src tests`が成功した。
- GitHub Dependabotを再確認し、全40件が`fixed`、オープン警告が0件であることを確認した。

## 残課題

- 依存更新後の配布用PyInstaller成果物が必要な場合は、別途ビルド確認を行う。

## 関連要件 / 関連設計

- 要件: 既存のPython依存関係と各機能の実行要件
- 設計: `docs/dev/architecture/`配下の各アプリ構成

## 実装着手前の自己レビュー結果

- 観点1: 依存更新の範囲が過大でないか
  - 判定: OK。GitHub警告の対象パッケージと直接依存の下限更新に限定した。
- 観点2: `uv.lock`だけでなくpip代替経路も更新されるか
  - 判定: OK。`pyproject.toml`と`requirements.txt`を同期した。
- 観点3: 実行時依存と開発依存を区別できるか
  - 判定: OK。実行時依存を優先し、開発・ビルド依存も警告対象として更新した。
