# PyInstallerビルド

本リポジトリはタグPushでGitHub Actionsがビルドします（`.github/workflows/build.yml`）。

## ローカルビルド例（Windows）

```powershell
# onedir
pyinstaller --noconsole --onedir -n Water-Info-Acquirer-win-d main.py --paths src --add-data "pyproject.toml;."

# onefile
pyinstaller --noconsole --onefile -n Water-Info-Acquirer-win-f main.py --paths src --add-data "pyproject.toml;."
```

!!! note "依存のインストール"
    ビルド前に依存関係をインストールしてください。`uv` を使う場合は `uv sync` でOKです。

