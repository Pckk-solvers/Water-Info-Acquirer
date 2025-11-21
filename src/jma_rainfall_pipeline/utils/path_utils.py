"""プロジェクトパスの解決を行うユーティリティヘルパー。"""
from pathlib import Path
from typing import Iterable


_DEFAULT_MARKERS = ("pyproject.toml", "setup.cfg", ".git")


def get_project_root(markers: Iterable[str] = _DEFAULT_MARKERS) -> Path:
    """プロジェクトルートディレクトリを返します。

    ルートは ``pyproject.toml`` や ``.git`` などの既知のプロジェクトマーカー
    ファイルを検索して決定されます。マーカーが見つからない場合は、このモジュールより
    3階層上のディレクトリにフォールバックし、現在のプロジェクトレイアウトでは
    リポジトリルートに対応します。
    """
    current = Path(__file__).resolve().parent

    for directory in [current, *current.parents]:
        if any((directory / marker).exists() for marker in markers):
            return directory

    # 既知のパッケージレイアウトに基づいてリポジトリルートにフォールバック
    return Path(__file__).resolve().parents[3]
