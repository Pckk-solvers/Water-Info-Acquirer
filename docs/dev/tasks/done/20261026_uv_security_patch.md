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

## 実施結果
- uvの最低版を0.13.5に設定し、CI/Docsのuv指定も0.13.5へ更新。
- TOML/YAML、uv lock --check --offline、git diff --checkの検証が成功。uv.lockと実行依存の変更なし。
- commit cc831825のCI（40249745419）、文書（40249745428）、依存グラフ（40249751774）が成功。
- CI内のRuff、river_metaのpytest、文書ビルド、PyInstaller smoke buildが成功。
- 既存のPython・仮想環境内のpipは今回一括更新していない。uv自体の更新と区別して開発手順に確認方法を記載。
- コード反映・検証が完了したためdoneへ移動。製品版数・公開済みReleaseの変更なし。
