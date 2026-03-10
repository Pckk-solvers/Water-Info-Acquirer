from __future__ import annotations

from river_meta.rainfall.domain.models import JMAStationInput, RainfallDataset, RainfallQuery, WaterInfoStationInput
from river_meta.rainfall.domain.normalizer import normalize_interval_token
from river_meta.rainfall.domain.usecase_models import RainfallRunInput
from river_meta.rainfall.support.common import (
    CancelFn,
    LogFn,
    append_cancelled_once as _append_cancelled_once,
    is_cancelled as _is_cancelled,
    noop_log as _noop_log,
)
from river_meta.rainfall.support.period import resolve_query_period as _resolve_query_period
import river_meta.rainfall.sources.fetch_jma as _rainfall_fetch_jma_service
import river_meta.rainfall.sources.fetch_water_info as _rainfall_fetch_waterinfo_service
import river_meta.rainfall.sources.station_resolution as _rainfall_station_resolution


def _resolve_jma_stations_for_config(config: RainfallRunInput, logger: LogFn) -> list[JMAStationInput]:
    return _rainfall_station_resolution.resolve_jma_stations_for_config(config, logger)


def _resolve_waterinfo_codes_for_config(config: RainfallRunInput, logger: LogFn) -> list[str]:
    return _rainfall_station_resolution.resolve_waterinfo_codes_for_config(config, logger)


def _resolve_sources(source: str) -> list[str]:
    return _rainfall_station_resolution.resolve_sources(source)


def run_rainfall_collect(
    config: RainfallRunInput,
    *,
    log: LogFn | None = None,
    should_stop: CancelFn | None = None,
) -> RainfallDataset:
    logger = log or _noop_log
    try:
        sources = _resolve_sources(config.source)
        interval = normalize_interval_token(config.interval)
        start_at, end_at = _resolve_query_period(config)
        query = RainfallQuery(start_at=start_at, end_at=end_at, interval=interval)

        if _is_cancelled(should_stop):
            return RainfallDataset(records=[], errors=["cancelled"])

        all_records = []
        all_errors = []
        for source in sources:
            if _is_cancelled(should_stop):
                _append_cancelled_once(all_errors)
                break
            if source == "jma":
                part = _collect_jma(config, query=query, logger=logger, should_stop=should_stop)
            else:
                part = _collect_waterinfo(config, query=query, logger=logger, should_stop=should_stop)
            all_records.extend(part.records)
            all_errors.extend(part.errors)

        all_records.sort(key=lambda item: (item.station_key, item.observed_at, item.source))
        return RainfallDataset(records=all_records, errors=all_errors)
    except Exception as exc:  # noqa: BLE001
        message = f"{type(exc).__name__}: {exc}"
        logger(message)
        return RainfallDataset(records=[], errors=[message])


def _collect_jma(
    config: RainfallRunInput,
    *,
    query: RainfallQuery,
    logger: LogFn,
    should_stop: CancelFn | None,
) -> RainfallDataset:
    try:
        stations = _resolve_jma_stations_for_config(config, logger)
        if not stations:
            raise ValueError("jma_stations or jma_station_codes or jma_prefectures is required for source=jma")
        return _collect_jma_with_resolved(
            stations,
            query,
            config.include_raw,
            logger,
            should_stop,
            config.jma_log_level,
            config.jma_enable_log_output,
        )
    except Exception as exc:  # noqa: BLE001
        message = f"jma:{type(exc).__name__}: {exc}"
        logger(message)
        return RainfallDataset(records=[], errors=[message])


def _collect_jma_with_resolved(
    stations: list[JMAStationInput],
    query: RainfallQuery,
    include_raw: bool,
    logger: LogFn,
    should_stop: CancelFn | None,
    jma_log_level: str | None,
    jma_enable_log_output: bool | None,
) -> RainfallDataset:
    return _rainfall_fetch_jma_service.collect_jma_with_resolved(
        stations,
        query,
        include_raw,
        logger,
        should_stop,
        jma_log_level,
        jma_enable_log_output,
    )


def _collect_waterinfo(
    config: RainfallRunInput,
    *,
    query: RainfallQuery,
    logger: LogFn,
    should_stop: CancelFn | None,
) -> RainfallDataset:
    try:
        station_codes = _resolve_waterinfo_codes_for_config(config, logger)
        if not station_codes:
            raise ValueError("waterinfo_station_codes or waterinfo_prefectures is required for source=water_info")
        stations = [WaterInfoStationInput(station_code=code) for code in station_codes]
        return _collect_waterinfo_with_resolved(stations, query, config.include_raw, logger, should_stop)
    except Exception as exc:  # noqa: BLE001
        message = f"water_info:{type(exc).__name__}: {exc}"
        logger(message)
        return RainfallDataset(records=[], errors=[message])


def _collect_waterinfo_with_resolved(
    stations: list[WaterInfoStationInput],
    query: RainfallQuery,
    include_raw: bool,
    logger: LogFn,
    should_stop: CancelFn | None,
) -> RainfallDataset:
    return _rainfall_fetch_waterinfo_service.collect_waterinfo_with_resolved(
        stations,
        query,
        include_raw,
        logger,
        should_stop,
    )
