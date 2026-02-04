import argparse
import sys
import os
import tkinter as tk
from pathlib import Path
from typing import Iterable, Optional

from src.app_names import get_app_title

from .entry import show_water, show_error


def _set_cwd_to_project_root() -> None:
    """凍結時は実行ファイル親、非凍結時はパッケージ親をCWDにする。"""
    if getattr(sys, "frozen", False):
        target = Path(sys.executable).resolve().parent
    else:
        target = Path(__file__).resolve().parent.parent
    try:
        os.chdir(target)
    except OSError:
        pass


def main(argv: Optional[Iterable[str]] = None) -> None:
    """CLI entry point shared by `python -m src.water_info` and `python -m src` (dev)."""
    parser = argparse.ArgumentParser(description=get_app_title(lang="jp"))
    parser.add_argument(
        '--single-sheet',
        action='store_true',
        help='1シートに全データを出力してテンプレートにマージ（デフォルトは年別+月別）'
    )
    args = parser.parse_args(argv)
    single_sheet_mode = args.single_sheet

    _set_cwd_to_project_root()
    root = tk.Tk()
    root.withdraw()

    def _on_close():
        root.destroy()

    try:
        show_water(parent=root, single_sheet_mode=single_sheet_mode, on_open_other=None, on_close=_on_close)
        root.mainloop()
    except Exception as e:
        show_error(str(e))


if __name__ == '__main__':
    main()
