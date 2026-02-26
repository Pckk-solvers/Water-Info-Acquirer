from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .models import JMAStationInput

_INDEX_FILENAME = "jma_station_index.json"


def default_station_index_path() -> Path:
    return Path(__file__).resolve().parents[1] / "resources" / _INDEX_FILENAME


def load_station_index(path: str | None = None) -> dict[str, Any]:
    index_path = Path(path) if path else default_station_index_path()
    if not index_path.exists():
        raise FileNotFoundError(f"JMA station index not found: {index_path}")
    with index_path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    by_block = data.get("by_block_no")
    if not isinstance(by_block, dict):
        raise ValueError(f"Invalid station index format: {index_path}")
    return data


@dataclass(slots=True, frozen=True)
class StationResolveIssue:
    code: str
    reason: str


def resolve_jma_stations_from_codes(
    station_codes: list[str],
    *,
    index_data: dict[str, Any] | None = None,
    index_path: str | None = None,
) -> tuple[list[JMAStationInput], list[StationResolveIssue]]:
    data = index_data if index_data is not None else load_station_index(index_path)
    by_block: dict[str, list[dict[str, Any]]] = data["by_block_no"]

    resolved: list[JMAStationInput] = []
    issues: list[StationResolveIssue] = []
    seen: set[tuple[str, str, str]] = set()

    for raw_code in station_codes:
        code = str(raw_code).strip()
        if not code:
            issues.append(StationResolveIssue(code=code, reason="empty_code"))
            continue

        candidates = by_block.get(code)
        
        # block_no で見つからない場合、新設した AMeDAS station_id (5桁など) で検索を試みる
        if not candidates:
            found_by_id = []
            for block_candidates in by_block.values():
                for c in block_candidates:
                    if str(c.get("station_id", "")).strip() == code:
                        found_by_id.append(c)
            if found_by_id:
                candidates = found_by_id

        if not candidates:
            issues.append(StationResolveIssue(code=code, reason="not_found"))
            continue
        rec = _resolve_candidate(code, candidates)
        if rec is None:
            issues.append(StationResolveIssue(code=code, reason="ambiguous"))
            continue

        key = (
            str(rec.get("prec_no", "")).zfill(2),
            str(rec.get("block_no", "")).strip(),
            str(rec.get("obs_type", "a1")).strip().lower() or "a1",
        )
        if key in seen:
            continue
        seen.add(key)
        resolved.append(
            JMAStationInput(
                prefecture_code=key[0],
                block_number=key[1],
                obs_type=key[2],
                station_name=str(rec.get("station_name", "")),
            )
        )

    return resolved, issues


def resolve_jma_stations_from_prefectures(
    prefectures: list[str],
    *,
    index_data: dict[str, Any] | None = None,
    index_path: str | None = None,
) -> tuple[list[JMAStationInput], list[str]]:
    data = index_data if index_data is not None else load_station_index(index_path)
    by_block: dict[str, list[dict[str, Any]]] = data["by_block_no"]

    pref_to_codes, pref_code_to_name = _build_pref_alias_map(by_block)
    target_pref_codes: set[str] = set()
    unknown: list[str] = []
    for raw in prefectures:
        token = _normalize_pref_input(raw)
        if not token:
            continue
        hits = pref_to_codes.get(token)
        if not hits:
            unknown.append(str(raw))
            continue
        target_pref_codes.update(hits)

    stations: list[JMAStationInput] = []
    seen: set[tuple[str, str, str]] = set()
    for candidates in by_block.values():
        for rec in candidates:
            pref_code = str(rec.get("prec_no", "")).zfill(2)
            if pref_code not in target_pref_codes:
                continue
            block_no = str(rec.get("block_no", "")).strip()
            obs_type = str(rec.get("obs_type", "a1")).strip().lower() or "a1"
            key = (pref_code, block_no, obs_type)
            if key in seen:
                continue
            seen.add(key)
            station_name = str(rec.get("station_name", "")).strip()
            if not station_name:
                station_name = pref_code_to_name.get(pref_code, "")
            stations.append(
                JMAStationInput(
                    prefecture_code=pref_code,
                    block_number=block_no,
                    obs_type=obs_type,
                    station_name=station_name,
                )
            )

    stations.sort(
        key=lambda s: (
            int(s.prefecture_code) if s.prefecture_code.isdigit() else 999,
            int(s.block_number) if s.block_number.isdigit() else 999999,
            s.obs_type,
        )
    )
    return stations, unknown


def resolve_jma_station_codes_from_prefectures(
    prefectures: list[str],
    *,
    index_data: dict[str, Any] | None = None,
    index_path: str | None = None,
) -> tuple[list[str], list[str]]:
    stations, unknown = resolve_jma_stations_from_prefectures(
        prefectures,
        index_data=index_data,
        index_path=index_path,
    )
    codes = sorted({station.block_number for station in stations}, key=lambda v: (0, int(v)) if v.isdigit() else (1, v))
    return codes, unknown


def _resolve_candidate(code: str, candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
    if len(candidates) == 1:
        return candidates[0]

    # 同一観測所が複数県に跨るケース（例: 富士山）を自動解決する。
    station_names = {str(item.get("station_name", "")).strip() for item in candidates}
    obs_types = {str(item.get("obs_type", "")).strip().lower() for item in candidates}
    block_numbers = {str(item.get("block_no", "")).strip() for item in candidates}
    if len(station_names) == 1 and len(obs_types) == 1 and block_numbers == {code}:
        return sorted(
            candidates,
            key=lambda item: (
                int(str(item.get("prec_no", "999")).zfill(3))
                if str(item.get("prec_no", "")).isdigit()
                else 999,
                str(item.get("prec_no", "")),
            ),
        )[0]

    return None


def _build_pref_alias_map(
    by_block: dict[str, list[dict[str, Any]]],
) -> tuple[dict[str, set[str]], dict[str, str]]:
    alias_to_codes: dict[str, set[str]] = {}
    code_to_name: dict[str, str] = {}
    for candidates in by_block.values():
        for rec in candidates:
            pref_code = str(rec.get("prec_no", "")).zfill(2)
            pref_name = str(rec.get("pref_name", "")).strip()
            if not pref_code:
                continue
            if pref_name and pref_code not in code_to_name:
                code_to_name[pref_code] = pref_name

            aliases = {pref_code}
            if pref_name:
                aliases.add(pref_name)
                aliases.add(_normalize_pref_input(pref_name))
                if pref_name.endswith(("都", "府", "県")) and len(pref_name) > 1:
                    aliases.add(_normalize_pref_input(pref_name[:-1]))
            for alias in aliases:
                if not alias:
                    continue
                alias_to_codes.setdefault(alias, set()).add(pref_code)

    return alias_to_codes, code_to_name


def _normalize_pref_input(value: str) -> str:
    token = str(value).strip().replace("\u3000", "")
    if token.isdigit():
        return token.zfill(2)
    return token
