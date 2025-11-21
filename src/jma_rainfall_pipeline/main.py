#!/usr/bin/env python3
"""JMAミニパイプライン - エントリポイント"""

from __future__ import annotations

import argparse
import os
import sys
import tkinter as tk
from pathlib import Path
from typing import Sequence


def _ensure_src_on_path() -> None:
    """プロジェクトルートをPythonパスに追加する。"""
    if getattr(sys, "frozen", False):
        project_root = Path(sys.executable).resolve().parent
    else:
        project_root = Path(__file__).resolve().parents[2]
    root_path = str(project_root)
    if root_path not in sys.path:
        sys.path.insert(0, root_path)


def _build_parser() -> argparse.ArgumentParser:
    """コマンドライン引数のパーサーを構成する。"""
    parser = argparse.ArgumentParser(
        prog="jma-rainfall-pipeline",
        description="JMAミニパイプライン、GUIエントリ",
    )
    parser.add_argument(
        "--dummy",
        action="store_true",
        help=argparse.SUPPRESS,  # 互換用のダミー（将来削除可）
    )
    return parser


def run(argv: Sequence[str] | None = None) -> int:
    """CLI入口（スタンドアロン起動用）。"""
    _ensure_src_on_path()
    parser = _build_parser()
    args = parser.parse_args(argv)

    from jma_rainfall_pipeline.gui.app import show_jma

    root = tk.Tk()
    root.withdraw()

    def _on_close():
        root.destroy()

    show_jma(parent=root, on_open_other=None, on_close=_on_close)
    root.mainloop()
    return 0


def main() -> None:
    """スクリプトのエントリポイント。"""
    sys.exit(run(sys.argv[1:]))


if __name__ == "__main__":
    main()
