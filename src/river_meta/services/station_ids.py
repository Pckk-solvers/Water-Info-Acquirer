from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable

from river_meta.station_ids.core import (
    DEFAULT_UA,
    build_session,
    collect_station_ids,
    fetch_master_options,
)
from river_meta.station_ids.extractors import (
    build_komoku_maps,
    build_prefecture_maps,
    filter_pref_options_by_codes,
    normalize_komoku_code,
    normalize_item_token,
    normalize_pref_token,
    parse_csv_values,
    resolve_prefecture_targets_by_names,
)
from river_meta.station_ids.writers import write_ids_txt, write_pref_csv


LogFn = Callable[[str], None]


@dataclass(slots=True)
class StationIdsRunInput:
    out: str = "station_ids.txt"
    out_pref_csv: str | None = None
    sleep: float = 0.3
    timeout: float = 20.0
    page_max: int = 5000
    user_agent: str = DEFAULT_UA
    komoku: str = "-1"
    item: str = ""
    ken: str = "-1"
    pref: list[str] = field(default_factory=list)
    pref_list: str = ""
    ken_list: str = ""
    suikei: str = "-00001"
    city: str = ""
    kasen: str = ""
    name: str = ""


@dataclass(slots=True)
class StationIdsRunResult:
    output_txt: str
    output_pref_csv: str | None
    total_ids: int
    pref_rows: int


def _noop_log(_: str) -> None:
    return None


def run_station_ids_collect(
    config: StationIdsRunInput,
    *,
    log: LogFn | None = None,
) -> StationIdsRunResult:
    logger = log or _noop_log
    base_params = {
        "CITY": config.city,
        "KASEN": config.kasen,
        "KOMOKU": config.komoku,
        "NAME": config.name,
        "SUIKEI": config.suikei,
    }

    session = build_session(config.user_agent)

    all_ids: set[str] = set()
    pref_rows: list[dict[str, str]] = []
    komoku_code_to_name: dict[str, str] = {}
    komoku_alias_to_item: dict[str, tuple[str, str]] = {}
    pref_options: list[tuple[str, str]] = []
    code_to_name: dict[str, str] = {}
    alias_to_pref: dict[str, tuple[str, str]] = {}

    pref_name_tokens = list(config.pref)
    pref_name_tokens.extend(parse_csv_values(config.pref_list))
    item_token = config.item.strip()
    need_pref_lookup = (
        config.ken == "all"
        or bool(pref_name_tokens)
        or config.out_pref_csv is not None
        or (config.ken not in {"-1", "all"} and not re.fullmatch(r"\d{4}", config.ken))
    )
    need_komoku_lookup = bool(item_token) or not re.fullmatch(r"-?[0-9]{1,2}", config.komoku)
    need_master_lookup = need_pref_lookup or need_komoku_lookup

    if need_master_lookup:
        try:
            pref_options, komoku_options = fetch_master_options(session, timeout=config.timeout)
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(f"[ERROR] マスタ一覧の取得に失敗しました: {exc}") from exc
        if need_pref_lookup:
            if not pref_options:
                raise ValueError("[ERROR] 都道府県一覧を抽出できませんでした。")
            code_to_name, alias_to_pref = build_prefecture_maps(pref_options)
        if need_komoku_lookup or item_token:
            if not komoku_options:
                raise ValueError("[ERROR] 観測項目一覧を抽出できませんでした。")
            komoku_code_to_name, komoku_alias_to_item = build_komoku_maps(komoku_options)

    komoku_code = normalize_komoku_code(config.komoku)
    komoku_name = ""
    if item_token:
        token = normalize_item_token(item_token)
        hit = komoku_alias_to_item.get(token)
        if not hit:
            raise ValueError(f"[ERROR] 観測項目を解決できませんでした: {config.item}")
        komoku_code, komoku_name = hit
        logger(f"[INFO] resolved item: {config.item} -> {komoku_code} ({komoku_name})")
    elif not re.fullmatch(r"-?[0-9]{1,2}", config.komoku):
        token = normalize_item_token(config.komoku)
        hit = komoku_alias_to_item.get(token)
        if not hit:
            raise ValueError(f"[ERROR] 観測項目を解決できませんでした: {config.komoku}")
        komoku_code, komoku_name = hit
        logger(f"[INFO] resolved komoku: {config.komoku} -> {komoku_code} ({komoku_name})")
    else:
        komoku_name = komoku_code_to_name.get(komoku_code, "")

    base_params["KOMOKU"] = komoku_code
    if komoku_name:
        logger(f"[INFO] KOMOKU={komoku_code} ({komoku_name})")
    else:
        logger(f"[INFO] KOMOKU={komoku_code}")

    if pref_name_tokens:
        targets, errors = resolve_prefecture_targets_by_names(
            pref_options=pref_options,
            alias_to_pref=alias_to_pref,
            code_to_name=code_to_name,
            pref_names=pref_name_tokens,
        )
        if errors:
            raise ValueError(f"[ERROR] 都道府県名/コードを解決できませんでした: {', '.join(errors)}")
        targets = filter_pref_options_by_codes(targets, config.ken_list)
        if not targets:
            raise ValueError("[ERROR] 対象都道府県が0件です。入力と --ken-list を確認してください。")
        output_pref_csv = config.out_pref_csv or "station_ids_by_pref.csv"
        _collect_by_prefecture_targets(
            targets=targets,
            all_ids=all_ids,
            pref_rows=pref_rows,
            base_params=base_params,
            session=session,
            timeout=config.timeout,
            sleep=config.sleep,
            page_max=config.page_max,
            logger=logger,
        )
        write_pref_csv(output_pref_csv, pref_rows)
        logger(f"[DONE] wrote prefecture CSV {len(pref_rows)} rows -> {output_pref_csv}")
    elif config.ken == "all":
        targets = filter_pref_options_by_codes(pref_options, config.ken_list)
        if not targets:
            raise ValueError("[ERROR] 対象都道府県が0件です。--ken-list を確認してください。")

        output_pref_csv = config.out_pref_csv or "station_ids_by_pref.csv"
        _collect_by_prefecture_targets(
            targets=targets,
            all_ids=all_ids,
            pref_rows=pref_rows,
            base_params=base_params,
            session=session,
            timeout=config.timeout,
            sleep=config.sleep,
            page_max=config.page_max,
            logger=logger,
        )
        write_pref_csv(output_pref_csv, pref_rows)
        logger(f"[DONE] wrote prefecture CSV {len(pref_rows)} rows -> {output_pref_csv}")
    else:
        output_pref_csv = config.out_pref_csv
        ken_code = config.ken
        ken_name = ""
        if ken_code not in {"-1"} and not re.fullmatch(r"\d{4}", ken_code):
            normalized = normalize_pref_token(ken_code)
            resolved = alias_to_pref.get(normalized)
            if not resolved:
                raise ValueError(f"[ERROR] 都道府県名を解決できませんでした: {ken_code}")
            ken_code, ken_name = resolved
            logger(f"[INFO] resolved prefecture: {config.ken} -> {ken_code} ({ken_name})")

        params = dict(base_params)
        params["KEN"] = ken_code
        ids, total = collect_station_ids(
            session,
            params=params,
            timeout=config.timeout,
            sleep_sec=config.sleep,
            page_max=config.page_max,
            info_log=logger,
            warn_log=logger,
        )
        all_ids |= set(ids)
        logger(f"[INFO] KEN={ken_code} total={total} ids={len(ids)}")

        if config.out_pref_csv:
            pref_name = "全国" if ken_code == "-1" else (ken_name or code_to_name.get(ken_code, ""))
            pref_rows = [
                {"ken_code": ken_code, "prefecture": pref_name, "station_id": station_id}
                for station_id in ids
            ]
            write_pref_csv(config.out_pref_csv, pref_rows)
            logger(f"[DONE] wrote prefecture CSV {len(pref_rows)} rows -> {config.out_pref_csv}")

    sorted_ids = sorted(all_ids, key=lambda value: (0, int(value)) if value.isdigit() else (1, value))
    write_ids_txt(config.out, sorted_ids)
    logger(f"[DONE] wrote {len(sorted_ids)} station ids -> {config.out}")
    return StationIdsRunResult(
        output_txt=config.out,
        output_pref_csv=output_pref_csv,
        total_ids=len(sorted_ids),
        pref_rows=len(pref_rows),
    )


def _collect_by_prefecture_targets(
    *,
    targets: list[tuple[str, str]],
    all_ids: set[str],
    pref_rows: list[dict[str, str]],
    base_params: dict[str, str],
    session,
    timeout: float,
    sleep: float,
    page_max: int,
    logger: LogFn,
) -> None:
    logger(f"[INFO] prefectures: {len(targets)}")
    for index, (ken_code, pref_name) in enumerate(targets, start=1):
        logger(f"[INFO] ({index}/{len(targets)}) KEN={ken_code} {pref_name}")
        params = dict(base_params)
        params["KEN"] = ken_code
        try:
            ids, total = collect_station_ids(
                session,
                params=params,
                timeout=timeout,
                sleep_sec=sleep,
                page_max=page_max,
                info_log=logger,
                warn_log=logger,
            )
        except Exception as exc:  # noqa: BLE001
            logger(f"[WARN] KEN={ken_code} failed: {exc}")
            continue

        all_ids |= set(ids)
        for station_id in ids:
            pref_rows.append({"ken_code": ken_code, "prefecture": pref_name, "station_id": station_id})
        logger(f"[INFO] KEN={ken_code} total={total} ids={len(ids)}")
