import argparse
from typing import Iterable, Optional

from src.main_datetime import WWRApp, show_error


def main(argv: Optional[Iterable[str]] = None) -> None:
    """CLI entry point shared by `python -m src` and `python main.py`."""
    parser = argparse.ArgumentParser(description='水文データ取得ツール')
    parser.add_argument(
        '--single-sheet',
        action='store_true',
        help='1シート目に全データを出力してテンプレートにマージする（デフォルトは年度と月毎）'
    )
    args = parser.parse_args(argv)
    single_sheet_mode = args.single_sheet

    try:
        WWRApp(single_sheet_mode=single_sheet_mode)
    except Exception as e:
        show_error(str(e))


if __name__ == '__main__':
    main()
