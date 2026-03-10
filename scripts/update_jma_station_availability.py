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

src_dir = Path(__file__).resolve().parents[1] / "src"
if str(src_dir) not in sys.path:
    sys.path.insert(0, str(src_dir))

from river_meta.rainfall.sources.jma.availability import fetch_available_years_hourly  # noqa: E402


logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

DEFAULT_INDEX_PATH = src_dir / "river_meta" / "resources" / "jma_station_index.json"


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="JMA 観測所インデックスに可用年(hourly)を補完する")
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
    parser.add_argument("--timeout", type=float, default=10.0, help="HTTP timeout")
    parser.add_argument("--sleep", type=float, default=0.1, help="request sleep(sec)")
    parser.add_argument("--max-count", type=int, default=0, help="処理上限(0は無制限)")
    parser.add_argument("--pref", action="append", default=[], help="対象都道府県コード/名称 (複数可)")
    parser.add_argument("--station-code", action="append", default=[], help="対象 block_no/station_id (複数可)")
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


def _station_matches_pref(station: dict, targets: set[str]) -> bool:
    if not targets:
        return True
    pref_code = str(station.get("prec_no", "")).zfill(2)
    pref_name = str(station.get("pref_name", "")).strip()
    normalized_name = pref_name[:-1] if pref_name.endswith(("都", "府", "県")) else pref_name
    return pref_code in targets or pref_name in targets or normalized_name in targets


def _station_matches_code(station: dict, targets: set[str]) -> bool:
    if not targets:
        return True
    block_no = str(station.get("block_no", "")).strip()
    station_id = str(station.get("station_id", "")).strip()
    return block_no in targets or station_id in targets


def _iter_targets(
    by_block_no: dict[str, list[dict]],
    *,
    pref_tokens: list[str],
    station_codes: list[str],
) -> list[dict]:
    pref_set = {str(value).strip() for value in pref_tokens if str(value).strip()}
    code_set = {str(value).strip() for value in station_codes if str(value).strip()}
    targets: list[dict] = []
    seen: set[tuple[str, str, str]] = set()

    for stations in by_block_no.values():
        for station in stations:
            key = (
                str(station.get("prec_no", "")).zfill(2),
                str(station.get("block_no", "")).strip(),
                str(station.get("obs_type", "a1")).strip().lower() or "a1",
            )
            if key in seen:
                continue
            seen.add(key)
            if not _station_matches_pref(station, pref_set):
                continue
            if not _station_matches_code(station, code_set):
                continue
            targets.append(station)

    targets.sort(key=lambda item: (str(item.get("prec_no", "")).zfill(2), str(item.get("block_no", "")).strip(), str(item.get("obs_type", "a1")).strip()))
    return targets


def run_update_jma_availability(
    *,
    mode: str,
    index_path: Path,
    output_path: Path,
    timeout_sec: float,
    sleep_sec: float,
    max_count: int,
    pref_tokens: list[str],
    station_codes: list[str],
) -> int:
    if not index_path.exists():
        print(f"Error: JSONが見つかりません: {index_path}")
        return 1

    data = _load_json(index_path)
    by_block_no = data.get("by_block_no")
    if not isinstance(by_block_no, dict):
        print(f"Error: by_block_no が見つかりません: {index_path}")
        return 1

    targets = _iter_targets(by_block_no, pref_tokens=pref_tokens, station_codes=station_codes)
    if max_count > 0:
        targets = targets[:max_count]

    print("--- JMA availability update ---")
    print(f"モード: {mode}")
    print(f"対象観測所数: {len(targets)}")

    success_count = 0
    failure_count = 0
    empty_count = 0

    try:
        for index, station in enumerate(targets, start=1):
            if index % 10 == 0 or index == 1:
                print(
                    f"[INFO] processing {index}/{len(targets)}: "
                    f"{station.get('prec_no', '')}-{station.get('block_no', '')}"
                )

            prec_no = str(station.get("prec_no", "")).zfill(2)
            block_no = str(station.get("block_no", "")).strip()
            existing_years = list(station.get("available_years_hourly", []))
            result = fetch_available_years_hourly(
                prec_no=prec_no,
                block_no=block_no,
                timeout_sec=timeout_sec,
            )

            if result.status == "indeterminate":
                failure_count += 1
                station["available_years_hourly"] = existing_years
                logger.warning(
                    "Availability indeterminate for %s-%s: %s",
                    prec_no,
                    block_no,
                    result.reason,
                )
            else:
                years = sorted(result.years)
                station["available_years_hourly"] = years
                station["availability_updated_at"] = datetime.now(timezone.utc).isoformat()
                success_count += 1
                if not years:
                    empty_count += 1

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
    print(f"  - 空年リスト: {empty_count}件")
    return 0 if failure_count == 0 else 1


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    index_path = Path(args.index)
    output_path = Path(args.output) if args.output else index_path
    return run_update_jma_availability(
        mode=args.mode,
        index_path=index_path,
        output_path=output_path,
        timeout_sec=args.timeout,
        sleep_sec=args.sleep,
        max_count=args.max_count,
        pref_tokens=list(args.pref),
        station_codes=list(args.station_code),
    )


if __name__ == "__main__":
    raise SystemExit(main())
