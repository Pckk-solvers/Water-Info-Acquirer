from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Sequence

from .domain.models import Options, Period, WaterInfoRequest
from .entry import WaterInfoOutputResult, run_cli_request_for_code, save_unified_records_ndjson


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="water-info",
        description="water_info データを取得して出力する CLI",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    fetch = subparsers.add_parser("fetch", help="観測所コードを指定して取得")
    fetch.add_argument("--code", action="append", required=True, help="観測所コード")
    fetch.add_argument("--mode", choices=("S", "R", "U"), required=True, help="取得項目")
    fetch.add_argument("--start", required=True, help="開始年月 YYYY-MM")
    fetch.add_argument("--end", required=True, help="終了年月 YYYY-MM")
    fetch.add_argument("--interval", choices=("hourly", "daily"), required=True)
    fetch.add_argument("--single-sheet", action="store_true", help="指定全期間シートを出力")
    fetch.add_argument("--ndjson", action="store_true", help="複数コードを1枚の NDJSON にまとめて出力")
    fetch.add_argument("--excel", dest="excel", action="store_true", default=True, help="Excel を出力")
    fetch.add_argument("--no-excel", dest="excel", action="store_false", help="Excel を出力しない")
    fetch.add_argument("--parquet", action="store_true", help="Parquet を出力")
    fetch.add_argument("--output-dir", default="", help="出力ルートディレクトリ")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command != "fetch":
        raise ValueError(f"unsupported command: {args.command}")
    return _run_fetch(args)


def _run_fetch(args: argparse.Namespace) -> int:
    request = WaterInfoRequest(
        period=_parse_period(args.start, args.end),
        mode_type=args.mode,
        options=Options(
            use_daily=args.interval == "daily",
            single_sheet=bool(args.single_sheet),
            export_excel=bool(args.excel),
            export_parquet=bool(args.parquet),
            export_ndjson=bool(args.ndjson),
        ),
    )
    output_dir = Path(args.output_dir) if str(args.output_dir).strip() else None

    successes: list[WaterInfoOutputResult] = []
    failures: list[tuple[str, str]] = []
    combined_unified_records: list[dict[str, object]] = []
    for code in args.code:
        try:
            result = run_cli_request_for_code(code=str(code), request=request, output_dir=output_dir)
            successes.append(result)
            combined_unified_records.extend(result.unified_records)
            print(
                json.dumps(
                    {
                        "code": result.code,
                        "station_name": result.station_name,
                        "excel": str(result.excel_path) if result.excel_path else "",
                        "parquet": str(result.parquet_path) if result.parquet_path else "",
                    },
                    ensure_ascii=False,
                )
            )
        except Exception as exc:
            failures.append((str(code), str(exc)))
            print(f"{code}: {exc}", file=sys.stderr)

    if successes and failures:
        _emit_combined_ndjson_if_requested(
            request=request,
            output_dir=output_dir,
            combined_unified_records=combined_unified_records,
        )
        return 2
    if failures:
        return 1
    _emit_combined_ndjson_if_requested(
        request=request,
        output_dir=output_dir,
        combined_unified_records=combined_unified_records,
    )
    return 0


def _parse_period(start_value: str, end_value: str) -> Period:
    start_year, start_month = _parse_year_month(start_value)
    end_year, end_month = _parse_year_month(end_value)
    return Period(
        year_start=start_year,
        year_end=end_year,
        month_start=f"{start_month}月",
        month_end=f"{end_month}月",
    )


def _parse_year_month(value: str) -> tuple[str, int]:
    token = str(value).strip()
    parts = token.split("-")
    if len(parts) != 2:
        raise SystemExit(f"年月は YYYY-MM 形式で指定してください: {value}")
    year, month = parts
    if not year.isdigit() or len(year) != 4:
        raise SystemExit(f"年は4桁の数字で指定してください: {value}")
    if not month.isdigit():
        raise SystemExit(f"月は数字で指定してください: {value}")
    month_value = int(month)
    if month_value < 1 or month_value > 12:
        raise SystemExit(f"月は 01-12 の範囲で指定してください: {value}")
    return year, month_value


def _emit_combined_ndjson_if_requested(
    *,
    request: WaterInfoRequest,
    output_dir: Path | None,
    combined_unified_records: list[dict[str, object]],
) -> None:
    if not request.options.export_ndjson or not combined_unified_records:
        return
    ndjson_path = _build_combined_ndjson_path(request=request, output_dir=output_dir)
    save_unified_records_ndjson(combined_unified_records, ndjson_path)
    print(json.dumps({"ndjson": str(ndjson_path)}, ensure_ascii=False))


def _build_combined_ndjson_path(*, request: WaterInfoRequest, output_dir: Path | None) -> Path:
    period = request.period
    metric = _metric_token(request.mode_type)
    interval = "1day" if request.options.use_daily else "1hour"
    file_name = (
        f"water_info_batch_{metric}_{interval}_"
        f"{period.year_start}{_to_month_number(period.month_start):02d}_"
        f"{period.year_end}{_to_month_number(period.month_end):02d}.ndjson"
    )
    root = Path("outputs") / "water_info" if output_dir is None else Path(output_dir)
    return root / file_name


def _metric_token(mode_type: str) -> str:
    if mode_type == "S":
        return "water_level"
    if mode_type == "R":
        return "discharge"
    return "rainfall"


def _to_month_number(token: str) -> int:
    return int(str(token).replace("月", ""))
