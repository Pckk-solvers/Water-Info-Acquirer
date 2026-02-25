from __future__ import annotations

import argparse
import sys
from typing import Sequence

from river_meta.services.amedas import (
    DEFAULT_PDF_PATH,
    AmedasRunInput,
    run_amedas_extract,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="river-ame-master",
        description="Extract AMeDAS station master table from PDF to CSV.",
    )
    parser.add_argument(
        "--in-pdf",
        default=DEFAULT_PDF_PATH,
        help=f"入力PDFパス（既定: {DEFAULT_PDF_PATH}）",
    )
    parser.add_argument(
        "--out-csv",
        default="data/out/ame_master_kinki.csv",
        help="出力CSVパス",
    )
    parser.add_argument(
        "--pref",
        action="append",
        default=[],
        help="都道府県名（複数指定可）",
    )
    parser.add_argument(
        "--pref-list",
        default="",
        help="都道府県名のカンマ区切り（例: 大阪,兵庫,京都）",
    )
    parser.add_argument(
        "--all-pref",
        action="store_true",
        help="都道府県フィルタを無効化して全件出力",
    )
    parser.add_argument("--encoding", default="utf-8-sig", help="出力CSV文字コード")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config = AmedasRunInput(
        in_pdf=args.in_pdf,
        out_csv=args.out_csv,
        pref=list(args.pref),
        pref_list=args.pref_list,
        all_pref=args.all_pref,
        encoding=args.encoding,
    )

    try:
        run_amedas_extract(config, log=lambda msg: print(msg, file=sys.stderr))
    except Exception as exc:  # noqa: BLE001
        print(f"[river-ame-master] fatal: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
