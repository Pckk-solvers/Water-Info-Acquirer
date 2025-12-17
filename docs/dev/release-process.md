# リリース手順（タグ駆動）

## 手順

1. バージョンを更新（例: `pyproject.toml` の `project.version`）
2. リリースノートを追加（`docs/releases/`）
3. `vX.Y.Z` のタグを作成して push

## 何が起きるか

- `.github/workflows/build.yml` が実行され、onedir/onefileのzipが生成されます。
- GitHub Release（ドラフト）が作成され、アセットが添付されます。

