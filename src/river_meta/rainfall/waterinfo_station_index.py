from __future__ import annotations

from typing import Callable

from river_meta.station_ids.core import DEFAULT_UA, build_session, collect_station_ids, fetch_master_options
from river_meta.station_ids.extractors import (
    build_komoku_maps,
    build_prefecture_maps,
    normalize_item_token,
    resolve_prefecture_targets_by_names,
)

LogFn = Callable[[str], None]


def _noop_log(_: str) -> None:
    return None


def resolve_waterinfo_station_codes_from_prefectures(
    prefectures: list[str],
    *,
    item: str = "雨量",
    timeout: float = 20.0,
    sleep_sec: float = 0.3,
    page_max: int = 5000,
    user_agent: str = DEFAULT_UA,
    suikei: str = "-00001",
    city: str = "",
    kasen: str = "",
    name: str = "",
    log: LogFn | None = None,
) -> tuple[list[str], list[str]]:
    logger = log or _noop_log
    pref_tokens = [str(raw).strip() for raw in prefectures if str(raw).strip()]
    if not pref_tokens:
        return [], []

    session = build_session(user_agent)
    pref_options, komoku_options = fetch_master_options(session, timeout=timeout)
    if not pref_options:
        raise ValueError("water_info prefecture options not found")

    code_to_name, alias_to_pref = build_prefecture_maps(pref_options)
    targets, unknown = resolve_prefecture_targets_by_names(
        pref_options=pref_options,
        alias_to_pref=alias_to_pref,
        code_to_name=code_to_name,
        pref_names=pref_tokens,
    )
    if not targets:
        return [], unknown

    komoku_code = _resolve_komoku_code(item=item, komoku_options=komoku_options)
    all_codes: set[str] = set()
    for index, (ken_code, pref_name) in enumerate(targets, start=1):
        logger(f"waterinfo_prefecture=({index}/{len(targets)}) KEN={ken_code} {pref_name}")
        params = {
            "CITY": city,
            "KASEN": kasen,
            "KOMOKU": komoku_code,
            "NAME": name,
            "SUIKEI": suikei,
            "KEN": ken_code,
        }
        ids, total = collect_station_ids(
            session,
            params=params,
            timeout=timeout,
            sleep_sec=sleep_sec,
            page_max=page_max,
            warn_log=logger,
        )
        all_codes.update(ids)
        logger(f"waterinfo_prefecture_result KEN={ken_code} total={total} ids={len(ids)}")

    codes = sorted(all_codes, key=lambda value: (0, int(value)) if value.isdigit() else (1, value))
    return codes, unknown


def _resolve_komoku_code(*, item: str, komoku_options: list[tuple[str, str]]) -> str:
    if not komoku_options:
        return "01"
    _, alias_to_item = build_komoku_maps(komoku_options)
    token = normalize_item_token(item)
    resolved = alias_to_item.get(token)
    if resolved:
        return resolved[0]
    rainfall_hit = alias_to_item.get(normalize_item_token("雨量"))
    if rainfall_hit:
        return rainfall_hit[0]
    return "01"
