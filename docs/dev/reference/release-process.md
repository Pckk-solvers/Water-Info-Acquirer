# リリース手順（タグ駆動）

## 手順

1. バージョンを更新（例: `pyproject.toml` の `project.version`）
2. リリースノートを追加（`docs/releases/`）
3. `vX.Y.Z` のタグを作成して push

## 何が起きるか

- `.github/workflows/build.yml` が実行され、固定名 spec を使って onedir / onefile をビルドします。
- version は `pyproject.toml` の `project.version` を読んで成果物名に反映されます。
- GitHub Release（ドラフト）が作成され、アセットが添付されます。

## ビルド定義

- onedir: `water_info_acquirer_onedir.spec`
- onefile: `water_info_acquirer_onefile.spec`

spec ファイル名自体には version を含めません。version は spec 内で `pyproject.toml` を読んで組み立てます。
