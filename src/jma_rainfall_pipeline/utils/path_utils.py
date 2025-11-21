"""プロジェクトパスの解決ユーティリティ兼ヘルパー。"""

from pathlib import Path
from typing import Iterable
import sys


_DEFAULT_MARKERS = ("pyproject.toml", "setup.cfg", ".git")


def get_project_root(markers: Iterable[str] = _DEFAULT_MARKERS) -> Path:
    """プロジェクトルートディレクトリを返す。

    - 凍結時（PyInstaller等）は実行ファイルの親を返す。
    - 非凍結時は pyproject.toml/.git などを上位に探し、見つからなければフォールバック。
    """
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent

    current = Path(__file__).resolve().parent

    for directory in [current, *current.parents]:
        if any((directory / marker).exists() for marker in markers):
            return directory

    # スクリプト配置からのフォールバック
    return Path(__file__).resolve().parents[3]
