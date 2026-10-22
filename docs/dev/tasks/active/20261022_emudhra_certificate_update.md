# eMudhra証明書問題への対応

## 目的

信頼ストアの旧いcertifiを更新し、ソース導入・配布EXEの両方で再導入を防ぐ。

## 対象ファイル

- pyproject.toml、uv.lock、requirements.txt
- .github/workflows/build.yml、ci.yml、docs.yml
- CHANGELOG.md

## 実施内容

- certifiの下限を2026.10.19へ設定し、対象パッケージのみ更新する。
- pipの初回更新はPython公式のSHA-256固定wheelを使用する。
- CIのuvを対策済み0.13.4へ固定し、1.4.1の配布物を既存workflowで作成する。

## 完了条件と確認方法

uv lock --check、依存差分の確認、certifi実体のCA指紋確認、既存テスト・CIの結果を記録する。
ビルド成功後、既存運用のdraft ReleaseとZIPを確認する。

## 関連要件・設計

- ../../requirements/water_info.md
- ../../architecture/overview.md
- ../../reference/build-pyinstaller.md

計算・時刻・出力の仕様は変更しない。依存と配布手順の保守として扱う。

## 実装着手前の自己レビュー

対象差分・安全な初回取得・requirements経由のビルド・版数整合を確認済み。
2026-10-22のユーザー指示で、依存更新・commit・push・可能なリリースを実施する。
