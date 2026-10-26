# uvの証明書対策追加パッチ

## 目的・関連要件
eMudhra/emSign問題の追加対策を含むuv 0.13.5以降を開発環境で要求し、CIでも0.13.5を使用する。
docs/advanced/run-with-uv.mdの開発手順を対象とする保守変更で、業務仕様・アーキテクチャ・実行依存は維持する。

## 対象ファイル・実施内容
- pyproject.toml: tool.uv.required-versionを>=0.13.5に設定。
- .github/workflows/ci.yml、docs.yml: uvの指定を0.13.5に更新。
- docs/advanced/run-with-uv.md: uvの必要バージョンと既存Python・pipの確認手順を記載。

## 完了条件・確認方法
TOML/YAMLの構文、uv設定とCIの指定、uv lock --check、git diff --check、push後のCIを確認する。
uv.lockと実行依存の版数は変更しない。新しい製品版の公開は行わない。

## 実装前の自己レビュー
設定差分・版数整合・既存仕様への影響を確認し、保守範囲として妥当と判断。
ユーザーの「前回対応したリポジトリ全部対応」の指示に基づき実施する。
