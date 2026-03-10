# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "beautifulsoup4",
#     "requests",
# ]
# ///

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

src_dir = Path(__file__).resolve().parents[1] / "src"
if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))

from river_meta.fetcher import fetch_html
from river_meta.parser_availability import parse_availability_page
from river_meta.station_ids.core import DEFAULT_UA, build_session


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_INDEX_PATH = src_dir / "river_meta" / "resources" / "waterinfo_station_index.json"
RAIN_URL = "https://www1.river.go.jp/cgi-bin/SrchRainData.exe?ID={station_id}&KIND={kind}&PAGE={page}"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="WaterInfo 観測所インデックスに可用年(hourly/daily)を補完する"
    )
    parser.add_argument(
        "--mode",
        choices=["update", "rebuild"],
        default="update",
        help="update: 既存値をフォールバックに使いながら更新 / rebuild: 全件再計算",
    )
    parser.add_argument(
        "--index",
        default=str(DEFAULT_INDEX_PATH),
        help=f"入力JSONパス (既定: {DEFAULT_INDEX_PATH})",
    )
    parser.add_argument(
        "--output",
        default="",
        help="出力JSONパス (未指定時は --index と同じ)",
    )
    parser.add_argument("--timeout", type=float, default=20.0, help="HTTP timeout")
    parser.add_argument("--sleep", type=float, default=0.3, help="request sleep(sec)")
    parser.add_argument("--page-scan-max", type=int, default=20, help="availability PAGE 探索上限")
    parser.add_argument("--max-count", type=int, default=0, help="処理上限(0は無制限)")
    parser.add_argument("--test-pref", type=str, default="", help="都道府県コード/名称で絞り込み")
    parser.add_argument("--station-id", action="append", default=[], help="対象観測所ID (複数可)")
    return parser.parse_args(argv)


def _load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _save_json(path: Path, data: dict) -> None:
    data["generated_at"] = datetime.now(timezone.utc).isoformat()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def _fetch_available_years(
    session: requests.Session,
    *,
    station_id: str,
    kind: int,
    timeout_sec: float,
    user_agent: str,
    page_scan_max: int,
    sleep_sec: float,
) -> tuple[list[int], bool, str]:
    all_years: list[int] = []
    saw_decade_table = False

    for page in range(page_scan_max + 1):
        url = RAIN_URL.format(station_id=station_id, kind=kind, page=page)
        result = fetch_html(session, url, timeout_sec=timeout_sec, user_agent=user_agent)
        if not result.ok or not result.html:
            reason = result.error or "fetch_failed"
            if result.status_code is not None:
                reason = f"{reason} (status={result.status_code})"
            return [], False, reason

        page_years, has_decade_table = parse_availability_page(result.html)
        if not has_decade_table:
            if page == 0:
                return [], False, "no_availability_table"
            break

        saw_decade_table = True
        all_years.extend(page_years)

        if sleep_sec > 0:
            time.sleep(sleep_sec)

    return sorted(set(all_years)), saw_decade_table, "ok"


def _iter_targets(
    by_station_id: dict[str, dict],
    *,
    test_pref: str,
    station_ids: list[str],
) -> list[tuple[str, dict]]:
    targets = list(by_station_id.items())

    if test_pref:
        token = str(test_pref).strip()
        targets = [
            (sid, meta)
            for sid, meta in targets
            if str(meta.get("pref_code", "")).strip() == token or str(meta.get("pref_name", "")).strip() == token
        ]

    if station_ids:
        selected = {str(value).strip() for value in station_ids if str(value).strip()}
        targets = [(sid, meta) for sid, meta in targets if sid in selected]

    targets.sort(key=lambda item: item[0])
    return targets


def run_update_waterinfo_availability(
    *,
    mode: str,
    index_path: Path,
    output_path: Path,
    timeout_sec: float,
    sleep_sec: float,
    page_scan_max: int,
    max_count: int,
    test_pref: str,
    station_ids: list[str],
) -> int:
    if not index_path.exists():
        print(f"Error: JSONが見つかりません: {index_path}")
        return 1

    data = _load_json(index_path)
    by_station_id = data.get("by_station_id")
    if not isinstance(by_station_id, dict):
        print(f"Error: by_station_id が見つかりません: {index_path}")
        return 1

    session = build_session(user_agent=DEFAULT_UA)
    targets = _iter_targets(
        by_station_id,
        test_pref=test_pref,
        station_ids=station_ids,
    )
    if max_count > 0:
        targets = targets[:max_count]

    print("--- WaterInfo availability update ---")
    print(f"モード: {mode}")
    print(f"対象観測所数: {len(targets)}")

    success_count = 0
    failure_count = 0

    try:
        for index, (station_id, meta) in enumerate(targets, start=1):
            if index % 10 == 0 or index == 1:
                print(f"[INFO] processing {index}/{len(targets)}: {station_id}")

            old_hourly = list(meta.get("available_years_hourly", [])) if isinstance(meta, dict) else []
            old_daily = list(meta.get("available_years_daily", [])) if isinstance(meta, dict) else []

            hourly_years, hourly_ok, hourly_reason = _fetch_available_years(
                session,
                station_id=station_id,
                kind=2,
                timeout_sec=timeout_sec,
                user_agent=DEFAULT_UA,
                page_scan_max=page_scan_max,
                sleep_sec=sleep_sec,
            )
            daily_years, daily_ok, daily_reason = _fetch_available_years(
                session,
                station_id=station_id,
                kind=3,
                timeout_sec=timeout_sec,
                user_agent=DEFAULT_UA,
                page_scan_max=page_scan_max,
                sleep_sec=sleep_sec,
            )

            if hourly_ok and daily_ok:
                meta["available_years_hourly"] = hourly_years
                meta["available_years_daily"] = daily_years
                meta["availability_updated_at"] = datetime.now(timezone.utc).isoformat()
                success_count += 1
            else:
                failure_count += 1
                if mode == "rebuild":
                    meta["available_years_hourly"] = old_hourly
                    meta["available_years_daily"] = old_daily
                else:
                    meta["available_years_hourly"] = old_hourly
                    meta["available_years_daily"] = old_daily
                logger.warning(
                    "Failed to update availability for %s hourly=(%s) daily=(%s)",
                    station_id,
                    hourly_reason,
                    daily_reason,
                )

            if sleep_sec > 0:
                time.sleep(sleep_sec)

            if index % 100 == 0:
                _save_json(output_path, data)

    except KeyboardInterrupt:
        logger.warning("Interrupted by user. Saving current progress...")
    finally:
        _save_json(output_path, data)

    print(f"完了: {output_path}")
    print(f"  - 成功: {success_count}件")
    print(f"  - 失敗: {failure_count}件")
    return 0 if failure_count == 0 else 1


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    index_path = Path(args.index)
    output_path = Path(args.output) if args.output else index_path
    return run_update_waterinfo_availability(
        mode=args.mode,
        index_path=index_path,
        output_path=output_path,
        timeout_sec=args.timeout,
        sleep_sec=args.sleep,
        page_scan_max=args.page_scan_max,
        max_count=args.max_count,
        test_pref=args.test_pref,
        station_ids=list(args.station_id),
    )


if __name__ == "__main__":
    raise SystemExit(main())
