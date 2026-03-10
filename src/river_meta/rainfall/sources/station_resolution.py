from __future__ import annotations

from typing import TYPE_CHECKING

from river_meta.rainfall.domain.models import JMAStationInput
from river_meta.rainfall.domain.normalizer import normalize_source_token
from river_meta.rainfall.sources.jma.station_index import (
    resolve_jma_stations_from_codes,
    resolve_jma_stations_from_prefectures,
)
from river_meta.rainfall.sources.water_info.station_index import resolve_waterinfo_station_codes_from_prefectures

from river_meta.rainfall.support.common import LogFn

if TYPE_CHECKING:
    from river_meta.rainfall.domain.usecase_models import RainfallRunInput


def dedupe_jma_stations(stations: list[JMAStationInput]) -> list[JMAStationInput]:
    deduped: list[JMAStationInput] = []
    seen: set[tuple[str, str, str]] = set()
    for station in stations:
        key = (station.prefecture_code, station.block_number, station.obs_type)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(station)
    return deduped


def dedupe_codes(codes: list[str]) -> list[str]:
    return sorted(set(codes), key=lambda value: (0, int(value)) if value.isdigit() else (1, value))


def resolve_sources(source: str) -> list[str]:
    token = str(source or "").strip().lower()
    if token in {"both", "all", "jma+water_info", "water_info+jma", "jma+waterinfo", "waterinfo+jma"}:
        return ["jma", "water_info"]
    return [normalize_source_token(source)]


def resolve_jma_stations_for_config(config: "RainfallRunInput", logger: LogFn) -> list[JMAStationInput]:
    return resolve_jma_stations_for_config_with_overrides(
        config,
        logger,
        resolve_by_prefectures=resolve_jma_stations_from_prefectures,
        resolve_by_codes=resolve_jma_stations_from_codes,
    )


def resolve_jma_stations_for_config_with_overrides(
    config: "RainfallRunInput",
    logger: LogFn,
    *,
    resolve_by_prefectures,
    resolve_by_codes,
) -> list[JMAStationInput]:
    stations: list[JMAStationInput] = []
    if config.jma_stations:
        stations.extend(
            [
                JMAStationInput(
                    prefecture_code=str(item[0]),
                    block_number=str(item[1]),
                    obs_type=(str(item[2]) if len(item) > 2 else "a1"),
                )
                for item in config.jma_stations
            ]
        )
    if config.jma_prefectures:
        from_prefs, pref_issues = resolve_by_prefectures(
            config.jma_prefectures,
            index_path=config.jma_station_index_path,
        )
        stations.extend(from_prefs)
        pref_codes = {station.block_number for station in from_prefs}
        logger(
            f"jma_prefectures={config.jma_prefectures} "
            f"resolved_station_codes={len(pref_codes)}"
        )
        for pref in pref_issues:
            logger(f"prefecture_resolve_error={pref}")
    if config.jma_station_codes:
        resolved, issues = resolve_by_codes(
            config.jma_station_codes,
            index_path=config.jma_station_index_path,
        )
        stations.extend(resolved)
        for issue in issues:
            logger(f"station_code={issue.code} resolve_error={issue.reason}")
    return dedupe_jma_stations(stations)


def resolve_waterinfo_codes_for_config(config: "RainfallRunInput", logger: LogFn) -> list[str]:
    return resolve_waterinfo_codes_for_config_with_overrides(
        config,
        logger,
        resolve_by_prefectures=resolve_waterinfo_station_codes_from_prefectures,
    )


def resolve_waterinfo_codes_for_config_with_overrides(
    config: "RainfallRunInput",
    logger: LogFn,
    *,
    resolve_by_prefectures,
) -> list[str]:
    station_codes = [str(code).strip() for code in config.waterinfo_station_codes if str(code).strip()]
    if config.waterinfo_prefectures:
        pref_codes, pref_issues = resolve_by_prefectures(
            config.waterinfo_prefectures,
            log=logger,
        )
        station_codes.extend(pref_codes)
        logger(
            f"waterinfo_prefectures={config.waterinfo_prefectures} "
            f"resolved_station_codes={len(pref_codes)}"
        )
        for pref in pref_issues:
            logger(f"prefecture_resolve_error={pref}")
    return dedupe_codes(station_codes)
