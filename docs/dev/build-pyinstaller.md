# PyInstallerビルド

本リポジトリはタグPushでGitHub Actionsがビルドします（`.github/workflows/build.yml`）。

## ローカルビルド例（Windows）

```powershell
uv sync

# onedir
uv run pyinstaller water_info_acquirer_onedir.spec

# onefile
uv run pyinstaller water_info_acquirer_onefile.spec
```

!!! note "依存のインストール"
    ビルド前に依存関係をインストールしてください。`uv` を使う場合は `uv sync` でOKです。

!!! note "成果物名"
    spec ファイル名は固定ですが、成果物名は `pyproject.toml` の `project.version` を読んで組み立てます。
    例:

    - `Water-Info-Acquirer-v1.2.0-win-d`
    - `Water-Info-Acquirer-v1.2.0-win-f.exe`
