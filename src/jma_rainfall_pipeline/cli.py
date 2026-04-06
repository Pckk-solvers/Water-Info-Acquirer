from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Sequence

from .controller.weather_data_controller import WeatherDataController, WeatherExportSummary
from .fetcher.jma_codes_fetcher import fetch_prefecture_codes, fetch_station_codes
from .logger.app_logger import set_runtime_log_options, setup_logging


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="jma-rainfall",
        description="JMA 降雨量データを一覧表示または取得する CLI",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_pref = subparsers.add_parser("list-prefectures", help="都道府県一覧を表示")
    list_pref.add_argument("--format", choices=("table", "json"), default="table")

    list_sta = subparsers.add_parser("list-stations", help="都道府県ごとの観測所一覧を表示")
    list_sta.add_argument("--pref", action="append", required=True, help="都道府県コードまたは名称")
    list_sta.add_argument("--format", choices=("table", "json"), default="table")

    fetch = subparsers.add_parser("fetch", help="JMA データを取得して出力")
    fetch.add_argument("--station", action="append", required=True, help="pref_code:block_no:obs_type")
    fetch.add_argument("--start", required=True, help="開始日 YYYY-MM-DD")
    fetch.add_argument("--end", required=True, help="終了日 YYYY-MM-DD")
    fetch.add_argument("--interval", choices=("daily", "hourly", "10min"), required=True)
    fetch.add_argument("--csv", action="store_true", help="CSV を出力")
    fetch.add_argument("--ndjson", action="store_true", help="NDJSON を出力")
    fetch.add_argument("--excel", dest="excel", action="store_true", default=True, help="Excel を出力")
    fetch.add_argument("--no-excel", dest="excel", action="store_false", help="Excel を出力しない")
    fetch.add_argument("--parquet", action="store_true", help="Parquet を出力")
    fetch.add_argument("--output-dir", default="", help="出力ルートディレクトリ")
    fetch.add_argument("--log", action="store_true", help="ログ出力を有効化")
    fetch.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"),
        default="INFO",
        help="ログレベル",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.command == "list-prefectures":
        return _run_list_prefectures(output_format=args.format)
    if args.command == "list-stations":
        return _run_list_stations(pref_tokens=list(args.pref), output_format=args.format)
    if args.command == "fetch":
        return _run_fetch(args)
    raise ValueError(f"unsupported command: {args.command}")


def _run_list_prefectures(*, output_format: str) -> int:
    prefectures = fetch_prefecture_codes()
    rows = [{"code": str(code).zfill(2), "name": name} for code, name in prefectures]
    _emit_rows(rows, headers=("code", "name"), output_format=output_format)
    return 0


def _run_list_stations(*, pref_tokens: list[str], output_format: str) -> int:
    pref_map = _build_prefecture_alias_map(fetch_prefecture_codes())
    rows: list[dict[str, str]] = []
    for token in pref_tokens:
        pref_code = _resolve_prefecture_token(token, pref_map)
        station_records = fetch_station_codes(pref_code)
        for record in station_records:
            obs_type = _normalize_obs_type(record.get("obs_method", ""))
            rows.append(
                {
                    "pref_code": pref_code,
                    "block_no": str(record.get("block_no", "")).strip(),
                    "station": str(record.get("station", "")).strip(),
                    "obs_type": obs_type,
                }
            )
    _emit_rows(rows, headers=("pref_code", "block_no", "station", "obs_type"), output_format=output_format)
    return 0


def _run_fetch(args: argparse.Namespace) -> int:
    if not args.csv and not args.excel and not args.parquet and not args.ndjson:
        raise SystemExit("少なくとも1つの出力形式を指定してください: --csv / --excel / --parquet / --ndjson")

    output_dir = Path(args.output_dir) if str(args.output_dir).strip() else None
    set_runtime_log_options(level=args.log_level, enable_log_output=args.log, logger_scope="jma")
    setup_logging(level_override=args.log_level, enable_log_output=args.log, logger_scope="jma")

    controller = WeatherDataController(interval=_parse_interval(args.interval))
    start_at = _parse_date(args.start)
    end_exclusive = _parse_end_exclusive(args.end)
    summary = controller.fetch_and_export_summary(
        stations=[_parse_station_token(token) for token in args.station],
        start=start_at,
        end=end_exclusive,
        output_dir=output_dir,
        export_csv=bool(args.csv),
        export_excel=bool(args.excel),
        export_parquet=bool(args.parquet),
        export_ndjson=bool(args.ndjson),
    )
    return _emit_fetch_summary(summary)


def _emit_fetch_summary(summary: WeatherExportSummary) -> int:
    if not summary.results:
        print("出力対象がありませんでした", file=sys.stderr)
        return 1

    for result in summary.results:
        payload = {
            "prec_no": result.prec_no,
            "block_no": result.block_no,
            "interval": result.interval_label,
            "csv": str(result.csv_path) if result.csv_path else "",
            "excel": str(result.excel_path) if result.excel_path else "",
            "parquet": str(result.parquet_path) if result.parquet_path else "",
            "ndjson": str(result.ndjson_path) if result.ndjson_path else "",
        }
        print(json.dumps(payload, ensure_ascii=False))
    return 0


def _parse_station_token(value: str) -> tuple[str, str, str]:
    parts = [part.strip() for part in str(value).split(":")]
    if len(parts) != 3:
        raise SystemExit(f"観測所指定は pref:block:obs_type の形式で指定してください: {value}")
    pref_code, block_no, obs_type = parts
    if not pref_code.isdigit():
        raise SystemExit(f"都道府県コードは数字で指定してください: {pref_code}")
    if not block_no.isdigit():
        raise SystemExit(f"観測所コードは数字で指定してください: {block_no}")
    return pref_code.zfill(2), block_no, _normalize_obs_type(obs_type)


def _normalize_obs_type(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if normalized in {"a", "a1"}:
        return "a1"
    if normalized in {"s", "s1"}:
        return "s1"
    raise SystemExit(f"obs_type は a/a1/s/s1 のいずれかで指定してください: {value}")


def _parse_date(value: str) -> datetime:
    try:
        parsed = datetime.strptime(str(value).strip(), "%Y-%m-%d")
    except ValueError as exc:
        raise SystemExit(f"日付は YYYY-MM-DD 形式で指定してください: {value}") from exc
    return parsed


def _parse_end_exclusive(value: str) -> datetime:
    return _parse_date(value) + timedelta(days=1)


def _parse_interval(value: str) -> timedelta:
    if value == "daily":
        return timedelta(days=1)
    if value == "hourly":
        return timedelta(hours=1)
    return timedelta(minutes=10)


def _build_prefecture_alias_map(prefectures: list[tuple[str, str]]) -> dict[str, str]:
    mapping: dict[str, str] = {}
    for code, name in prefectures:
        normalized_code = str(code).zfill(2)
        mapping[normalized_code] = normalized_code
        mapping[str(int(normalized_code))] = normalized_code
        mapping[str(name).strip()] = normalized_code
    return mapping


def _resolve_prefecture_token(token: str, pref_map: dict[str, str]) -> str:
    normalized = str(token).strip()
    resolved = pref_map.get(normalized)
    if resolved is None:
        raise SystemExit(f"都道府県を解決できませんでした: {token}")
    return resolved


def _emit_rows(
    rows: list[dict[str, str]],
    *,
    headers: tuple[str, ...],
    output_format: str,
) -> None:
    if output_format == "json":
        print(json.dumps(rows, ensure_ascii=False, indent=2))
        return
    if not rows:
        print("(no rows)")
        return
    widths = {header: max(len(header), *(len(str(row.get(header, ""))) for row in rows)) for header in headers}
    print(" ".join(header.ljust(widths[header]) for header in headers))
    for row in rows:
        print(" ".join(str(row.get(header, "")).ljust(widths[header]) for header in headers))
