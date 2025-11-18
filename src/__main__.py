import argparse
from datetime import datetime
from typing import Iterable, Optional

from src.bootstrap import build_app_service
from src.core.app import ExecutionOptions
from src.main_datetime import WWRApp, show_error


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='水文データ取得ツール (CLI)')
    parser.add_argument('--code', action='append', required=True, help='観測所コード（複数指定可）')
    parser.add_argument('--mode', choices=['S', 'R', 'U'], required=True, help='モード (S/R/U)')
    parser.add_argument('--start', required=True, help='取得開始年月 (YYYY-MM)')
    parser.add_argument('--end', required=True, help='取得終了年月 (YYYY-MM)')
    parser.add_argument('--single-sheet', action='store_true', help='単一シート出力')
    parser.add_argument('--daily', action='store_true', help='日別データを使用する場合のフラグ')
    return parser


def _parse_month(value: str) -> datetime:
    try:
        return datetime.strptime(value, '%Y-%m')
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f'年月の形式が正しくありません: {value}') from exc


def _parse_options(argv: Optional[Iterable[str]]) -> ExecutionOptions:
    parser = _build_parser()
    args = parser.parse_args(argv)
    start = _parse_month(args.start).date().replace(day=1)
    end = _parse_month(args.end).date().replace(day=1)
    if start > end:
        parser.error('開始年月は終了年月以前で指定してください')
    return ExecutionOptions(
        codes=args.code,
        period_start=start,
        period_end=end,
        mode=args.mode,
        single_sheet=args.single_sheet,
        use_daily=args.daily,
    )


def main(argv: Optional[Iterable[str]] = None) -> None:
    """CLI entry point shared by `python -m src` and `python main.py`."""
    app_service = build_app_service()
    if app_service is None:
        try:
            WWRApp(single_sheet_mode=False)
        except Exception as e:
            show_error(str(e))
        return

    try:
        options = _parse_options(argv)
        result = app_service.execute(options)
        print(f'出力ファイル: {result.file_path}')
    except Exception as exc:
        show_error(str(exc))


if __name__ == '__main__':
    main()
