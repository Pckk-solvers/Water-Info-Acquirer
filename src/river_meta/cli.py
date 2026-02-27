from __future__ import annotations

import argparse
import sys
from typing import Sequence

from .service import scrape_station
from .services.river_meta import RiverMetaRunInput, collect_station_ids, run_river_meta


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="river-meta",
        description="Scrape river.go.jp station metadata and rain availability.",
    )
    parser.add_argument(
        "--id",
        dest="station_ids",
        action="append",
        help="観測所記号（複数指定可）",
    )
    parser.add_argument(
        "--id-file",
        dest="id_file",
        help="観測所記号リスト（txt:1行1ID / csv:1列）",
    )
    parser.add_argument("--out", dest="output_path", help="Markdown保存先")
    parser.add_argument(
        "--out-dir-md",
        dest="output_dir_md",
        help="観測所ごとのMarkdown保存先ディレクトリ",
    )
    parser.add_argument("--out-csv", dest="output_csv_path", help="CSV保存先")
    parser.add_argument(
        "--kinds",
        nargs="+",
        type=int,
        default=[2, 3],
        help="取得対象のKIND (既定: 2 3)",
    )
    parser.add_argument(
        "--page-scan-max",
        type=int,
        default=2,
        help="PAGE探索上限 (既定: 2)",
    )
    parser.add_argument(
        "--timeout-sec",
        type=float,
        default=10,
        help="HTTPタイムアウト秒 (既定: 10)",
    )
    parser.add_argument("--user-agent", default=None, help="User-Agent")
    parser.add_argument(
        "--request-interval-ms",
        type=int,
        default=0,
        help="リクエスト間隔ミリ秒 (既定: 0)",
    )
    parser.add_argument(
        "--csv-encoding",
        default="utf-8-sig",
        help="CSV文字コード (既定: utf-8-sig)",
    )
    parser.add_argument(
        "--csv-delimiter",
        default=",",
        help="CSV区切り文字 (既定: ,)",
    )
    parser.add_argument(
        "--debug-save-html",
        default=None,
        help="取得HTML保存先ディレクトリ",
    )
    return parser


def _collect_station_ids(
    station_ids: list[str] | None,
    id_file: str | None,
) -> list[str]:
    return collect_station_ids(station_ids, id_file)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    config = RiverMetaRunInput(
        station_ids=args.station_ids,
        id_file=args.id_file,
        output_path=args.output_path,
        output_dir_md=args.output_dir_md,
        output_csv_path=args.output_csv_path,
        kinds=tuple(args.kinds),
        page_scan_max=args.page_scan_max,
        timeout_sec=args.timeout_sec,
        user_agent=args.user_agent,
        request_interval_ms=args.request_interval_ms,
        csv_encoding=args.csv_encoding,
        csv_delimiter=args.csv_delimiter,
        debug_save_html=args.debug_save_html,
    )

    try:
        result = run_river_meta(
            config,
            scrape_station_fn=scrape_station,
            log_info=lambda msg: print(msg, file=sys.stderr),
            log_warn=lambda msg: print(msg, file=sys.stderr),
            log_error=lambda msg: print(msg, file=sys.stderr),
        )
    except ValueError as exc:
        parser.error(str(exc))
        return 2

    if result.single_markdown:
        print(result.single_markdown, end="")
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
