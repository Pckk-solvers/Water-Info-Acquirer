from __future__ import annotations

import argparse

from river_meta.services.station_ids import StationIdsRunInput, run_station_ids_collect
from river_meta.station_ids.core import DEFAULT_UA


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="station_ids.txt", help="出力txtパス")
    parser.add_argument(
        "--out-pref-csv",
        default=None,
        help="都道府県付きCSV出力パス（ken=all時の既定: station_ids_by_pref.csv）",
    )
    parser.add_argument("--sleep", type=float, default=0.3, help="リクエスト間隔（秒）")
    parser.add_argument("--timeout", type=float, default=20.0, help="HTTPタイムアウト（秒）")
    parser.add_argument("--page-max", type=int, default=5000, help="安全用の最大PAGE（保険）")
    parser.add_argument("--user-agent", default=DEFAULT_UA, help="User-Agent")
    parser.add_argument("--komoku", default="-1")
    parser.add_argument(
        "--item",
        default="",
        help="観測項目名またはコード（例: 雨量, 水位流量, 01）",
    )
    parser.add_argument(
        "--ken",
        default="-1",
        help="都道府県コード（互換用。例: 2701）/ -1=全国 / all=全都道府県",
    )
    parser.add_argument(
        "--pref",
        action="append",
        default=[],
        help="都道府県名またはコード（複数指定可。例: --pref 大阪 --pref 東京）",
    )
    parser.add_argument(
        "--pref-list",
        default="",
        help="都道府県名またはコードのカンマ区切り（例: 大阪,東京,千葉）",
    )
    parser.add_argument(
        "--ken-list",
        default="",
        help="ken=all 時の対象コード絞り込み（カンマ区切り。例: 1301,2701）",
    )
    parser.add_argument("--suikei", default="-00001")
    parser.add_argument("--city", default="")
    parser.add_argument("--kasen", default="")
    parser.add_argument("--name", default="")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    config = StationIdsRunInput(
        out=args.out,
        out_pref_csv=args.out_pref_csv,
        sleep=args.sleep,
        timeout=args.timeout,
        page_max=args.page_max,
        user_agent=args.user_agent,
        komoku=args.komoku,
        item=args.item,
        ken=args.ken,
        pref=list(args.pref),
        pref_list=args.pref_list,
        ken_list=args.ken_list,
        suikei=args.suikei,
        city=args.city,
        kasen=args.kasen,
        name=args.name,
    )
    try:
        run_station_ids_collect(config, log=print)
    except Exception as exc:  # noqa: BLE001
        print(str(exc))
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
