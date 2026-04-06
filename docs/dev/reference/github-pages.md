# GitHub Pages公開

本リポジトリは `mkdocs-material` + GitHub Actions で GitHub Pages へデプロイします。

## 初回だけやること（リポジトリ設定）

1. GitHub のリポジトリで **Settings → Pages** を開く
2. **Build and deployment → Source** を **GitHub Actions** にする

## デプロイの流れ

- `main` ブランチに push すると、`.github/workflows/docs.yml` が走ります。
- `uv run mkdocs build` で `site/` を生成し、GitHub Pages へデプロイします。

## ローカル確認

```powershell
uv run mkdocs serve
```

## URL

プロジェクトページのURLは通常次になります。

- `https://pckk-solvers.github.io/Water-Info-Acquirer/`

