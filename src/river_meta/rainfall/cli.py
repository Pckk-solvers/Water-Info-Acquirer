from __future__ import annotations

import argparse
import inspect
import signal
import sys
import threading
from datetime import datetime
from typing import Sequence

from river_meta.services.rainfall import (
    RainfallGenerateInput,
    RainfallRunInput,
    run_rainfall_analyze,
    run_rainfall_collect,
    run_rainfall_generate,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="river-rainfall",
        description="Collect/analyze rainfall data from JMA and water_info.",
    )
    parser.add_argument("--mode", choices=["collect", "analyze", "generate"], default="analyze", help="実行モード")
    parser.add_argument("--source", default=None, help="データソース (jma / water_info / both)。generate時は不要")
    parser.add_argument("--interval", default="1hour", help="集計間隔 (10min / 1hour / 1day)")

    parser.add_argument("--year", type=int, default=None, help="対象年（例: 2025）")
    parser.add_argument("--start-at", default=None, help="開始日時（例: 2025-01-01 00:00:00）")
    parser.add_argument("--end-at", default=None, help="終了日時（例: 2025-12-31 23:59:59）")

    parser.add_argument("--jma-pref", action="append", default=[], help="JMA 都道府県名/コード（複数可）")
    parser.add_argument("--jma-pref-list", default="", help="JMA 都道府県名/コード CSV")
    parser.add_argument("--jma-station-code", action="append", default=[], help="JMA 観測所コード（複数可）")
    parser.add_argument("--jma-station-code-list", default="", help="JMA 観測所コード CSV")
    parser.add_argument("--jma-station-index-path", default=None, help="JMA 観測所インデックス JSON パス")

    parser.add_argument("--waterinfo-pref", action="append", default=[], help="water_info 都道府県名/コード（複数可）")
    parser.add_argument("--waterinfo-pref-list", default="", help="water_info 都道府県名/コード CSV")
    parser.add_argument(
        "--waterinfo-station-code",
        action="append",
        default=[],
        help="water_info 観測所コード（複数可）",
    )
    parser.add_argument("--waterinfo-station-code-list", default="", help="water_info 観測所コード CSV")

    parser.add_argument("--include-raw", action="store_true", help="生データ列を保持")
    parser.add_argument(
        "--jma-log-level",
        default=None,
        help="JMAログレベル（DEBUG/INFO/WARNING/ERROR/CRITICAL）",
    )
    parser.add_argument(
        "--jma-log-output",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="JMAログ出力の有効/無効（--jma-log-output / --no-jma-log-output）",
    )
    parser.add_argument("--export-excel", action="store_true", help="analyze 時にExcelを出力")
    parser.add_argument("--export-chart", action="store_true", help="analyze 時に降雨グラフPNGを出力")
    parser.add_argument(
        "--use-diff-mode",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="generate 時に差分更新を使う（--use-diff-mode / --no-use-diff-mode）。既定ON",
    )
    parser.add_argument(
        "--force-full-regenerate",
        action="store_true",
        help="generate 時に全再生成を強制する（差分更新設定より優先）。既定OFF",
    )
    parser.add_argument("--output-dir", required=True, help="出力ディレクトリ（必須）")
    parser.add_argument("--decimal-places", type=int, default=2, help="Excel の小数桁数")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        config = _build_run_input(args)
    except ValueError as exc:
        parser.error(str(exc))
        return 2

    stop_event = threading.Event()
    previous_handler = signal.getsignal(signal.SIGINT)

    def _handle_sigint(signum, frame) -> None:  # noqa: ANN001, ARG001
        if stop_event.is_set():
            raise KeyboardInterrupt
        print("[INFO] 停止要求を受け付けました。処理の区切りで停止します。", file=sys.stderr)
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_sigint)
    try:
        if args.mode == "generate":
            if args.force_full_regenerate:
                print(
                    "[INFO] --force-full-regenerate が有効のため、差分更新設定より全再生成を優先します。",
                    file=sys.stderr,
                )
            gen_config = _build_generate_input(args)
            gen_result = run_rainfall_generate(
                gen_config,
                log=lambda msg: print(msg, file=sys.stderr),
                should_stop=stop_event.is_set,
            )
            _print_generate_result(gen_result)
            return _exit_code_from_errors(gen_result.errors)

        if args.source is None:
            parser.error("collect/analyze モードでは --source が必須です。")
            return 2

        if args.mode == "collect":
            dataset = run_rainfall_collect(
                config,
                log=lambda msg: print(msg, file=sys.stderr),
                should_stop=stop_event.is_set,
            )
            _print_collect_result(dataset)
            return _exit_code_from_errors(dataset.errors)

        result = run_rainfall_analyze(
            config,
            export_excel=args.export_excel,
            export_chart=args.export_chart,
            output_dir=args.output_dir,
            decimal_places=args.decimal_places,
            log=lambda msg: print(msg, file=sys.stderr),
            should_stop=stop_event.is_set,
        )
        _print_analyze_result(result)
        return _exit_code_from_errors(result.dataset.errors)
    except KeyboardInterrupt:
        print("[INFO] 中断されました。", file=sys.stderr)
        return 130
    finally:
        signal.signal(signal.SIGINT, previous_handler)


def _build_run_input(args: argparse.Namespace) -> RainfallRunInput:
    start_at = _parse_datetime(args.start_at, is_end=False) if args.start_at else None
    end_at = _parse_datetime(args.end_at, is_end=True) if args.end_at else None

    if args.year is None:
        if start_at is None or end_at is None:
            raise ValueError("year を指定しない場合は --start-at と --end-at の両方が必要です。")
    else:
        if start_at is not None or end_at is not None:
            raise ValueError("year 指定時は --start-at/--end-at を併用できません。")

    return RainfallRunInput(
        source=args.source,
        start_at=start_at,
        end_at=end_at,
        year=args.year,
        interval=args.interval,
        jma_prefectures=_merge_csv_values(args.jma_pref, args.jma_pref_list),
        jma_station_codes=_merge_csv_values(args.jma_station_code, args.jma_station_code_list),
        waterinfo_prefectures=_merge_csv_values(args.waterinfo_pref, args.waterinfo_pref_list),
        waterinfo_station_codes=_merge_csv_values(args.waterinfo_station_code, args.waterinfo_station_code_list),
        jma_station_index_path=args.jma_station_index_path,
        jma_log_level=_optional_token(args.jma_log_level),
        jma_enable_log_output=args.jma_log_output,
        include_raw=bool(args.include_raw),
    )


def _build_generate_input(args: argparse.Namespace) -> RainfallGenerateInput:
    use_diff_mode = bool(args.use_diff_mode)
    force_full_regenerate = bool(args.force_full_regenerate)
    if force_full_regenerate:
        use_diff_mode = False

    kwargs: dict[str, object] = {
        "parquet_dir": args.output_dir,
        "export_excel": args.export_excel,
        "export_chart": args.export_chart,
        "decimal_places": args.decimal_places,
    }
    if _supports_generate_input_arg("use_diff_mode"):
        kwargs["use_diff_mode"] = use_diff_mode
    if _supports_generate_input_arg("force_full_regenerate"):
        kwargs["force_full_regenerate"] = force_full_regenerate
    return RainfallGenerateInput(**kwargs)


def _supports_generate_input_arg(arg_name: str) -> bool:
    try:
        parameters = inspect.signature(RainfallGenerateInput).parameters.values()
    except (TypeError, ValueError):
        return False
    names = {parameter.name for parameter in parameters}
    if arg_name in names:
        return True
    return any(parameter.kind is inspect.Parameter.VAR_KEYWORD for parameter in parameters)


def _merge_csv_values(values: list[str], csv_values: str) -> list[str]:
    merged = [str(value).strip() for value in values if str(value).strip()]
    merged.extend([item.strip() for item in str(csv_values).split(",") if item.strip()])
    deduped: list[str] = []
    seen: set[str] = set()
    for value in merged:
        if value in seen:
            continue
        seen.add(value)
        deduped.append(value)
    return deduped


def _optional_token(value: str | None) -> str | None:
    if value is None:
        return None
    token = str(value).strip()
    return token if token else None


def _parse_datetime(value: str, *, is_end: bool) -> datetime:
    raw = value.strip()
    if not raw:
        raise ValueError("日時文字列が空です。")
    formats = [
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%dT%H:%M",
        "%Y-%m-%d",
    ]
    for fmt in formats:
        try:
            parsed = datetime.strptime(raw, fmt)
        except ValueError:
            continue
        if fmt == "%Y-%m-%d" and is_end:
            return parsed.replace(hour=23, minute=59, second=59)
        return parsed
    raise ValueError(f"日時形式を解釈できませんでした: {value}")


def _exit_code_from_errors(errors: list[str]) -> int:
    if not errors:
        return 0
    if any(str(item).strip().lower() == "cancelled" for item in errors):
        return 130
    return 2


def _print_collect_result(dataset) -> None:
    print(f"records={len(dataset.records)}", file=sys.stderr)
    if dataset.errors:
        for error in dataset.errors:
            print(f"error={error}", file=sys.stderr)


def _print_analyze_result(result) -> None:
    print(f"records={len(result.dataset.records)}", file=sys.stderr)
    print(f"timeseries_rows={len(result.timeseries_df)}", file=sys.stderr)
    print(f"annual_max_rows={len(result.annual_max_df)}", file=sys.stderr)
    if result.excel_paths:
        for path in result.excel_paths:
            print(f"excel={path}", file=sys.stderr)
    if result.chart_paths:
        for path in result.chart_paths:
            print(f"chart={path}", file=sys.stderr)
    if result.dataset.errors:
        for error in result.dataset.errors:
            print(f"error={error}", file=sys.stderr)


def _print_generate_result(result) -> None:
    print(f"parquet_entries={len(result.entries)}", file=sys.stderr)
    print(f"complete={len(result.entries) - len(result.incomplete_entries)}", file=sys.stderr)
    print(f"incomplete={len(result.incomplete_entries)}", file=sys.stderr)
    if result.excel_paths:
        for path in result.excel_paths:
            print(f"excel={path}", file=sys.stderr)
    if result.chart_paths:
        for path in result.chart_paths:
            print(f"chart={path}", file=sys.stderr)
    if result.errors:
        for error in result.errors:
            print(f"error={error}", file=sys.stderr)


if __name__ == "__main__":
    raise SystemExit(main())
