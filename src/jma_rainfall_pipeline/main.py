#!/usr/bin/env python3
"""JMAミニパイプライン - エントリポイント"""

from __future__ import annotations

import argparse
import os
import sys
import tkinter as tk
from typing import Sequence


def _ensure_src_on_path() -> None:
    """srcディレクトリをPythonパスに追加する。"""
    current_dir = os.path.dirname(os.path.abspath(__file__))
    src_dir = os.path.dirname(current_dir)
    if src_dir not in sys.path:
        sys.path.insert(0, src_dir)


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
