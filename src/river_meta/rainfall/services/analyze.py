from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Callable

from river_meta.rainfall.outputs.analysis import build_annual_max_dataframe, build_hourly_timeseries_dataframe
from river_meta.rainfall.outputs.chart_exporter import export_rainfall_charts
from river_meta.rainfall.outputs.excel_exporter import export_station_rainfall_excel
from river_meta.rainfall.sources.jma.availability import fetch_available_years_hourly
from river_meta.rainfall.domain.models import JMAStationInput, RainfallDataset, WaterInfoStationInput
from river_meta.rainfall.domain.normalizer import normalize_interval_token
from river_meta.rainfall.domain.usecase_models import RainfallAnalyzeResult, RainfallRunInput
from river_meta.rainfall.support.common import (
    CancelFn,
    LogFn,
    append_cancelled_once as _append_cancelled_once,
    is_cancelled as _is_cancelled,
    noop_log as _noop_log,
)
from river_meta.rainfall.support.period import (
    format_station_target_period_log as _format_station_target_period_log,
    format_target_years_normalization_log as _format_target_years_normalization_log,
    resolve_target_years_for_analyze as _resolve_target_years_for_analyze,
)
import river_meta.rainfall.sources.fetch_jma as _rainfall_fetch_jma_service
import river_meta.rainfall.sources.fetch_water_info as _rainfall_fetch_waterinfo_service
import river_meta.rainfall.sources.station_resolution as _rainfall_station_resolution

if TYPE_CHECKING:
    import pandas as pd


def _resolve_jma_stations_for_config(config: RainfallRunInput, logger: LogFn) -> list[JMAStationInput]:
    return _rainfall_station_resolution.resolve_jma_stations_for_config(config, logger)


def _resolve_waterinfo_codes_for_config(config: RainfallRunInput, logger: LogFn) -> list[str]:
    return _rainfall_station_resolution.resolve_waterinfo_codes_for_config(config, logger)


def _resolve_sources(source: str) -> list[str]:
    return _rainfall_station_resolution.resolve_sources(source)


def run_rainfall_analyze(
    config: RainfallRunInput,
    *,
    export_excel: bool = False,
    export_chart: bool = False,
    output_dir: str = "outputs/river_meta/rainfall",
    decimal_places: int = 2,
    log: LogFn | None = None,
    should_stop: CancelFn | None = None,
) -> RainfallAnalyzeResult:
    import pandas as pd

    logger = log or _noop_log

    if normalize_interval_token(config.interval) != "1hour":
        dataset = RainfallDataset(records=[], errors=["run_rainfall_analyze supports interval='1hour' only"])
        empty = pd.DataFrame()
        return RainfallAnalyzeResult(dataset=dataset, timeseries_df=empty, annual_max_df=empty, excel_paths=[], chart_paths=[])

    all_errors: list[str] = []
    excel_paths: set[str] = set()
    chart_paths: list[str] = []
    created_parquet_paths: list[Path] = []
    jma_requested_year_total = 0
    jma_filtered_year_total = 0
    target_years = _resolve_target_years_for_analyze(config)
    logger(f"[collect] 取得順序: {config.collection_order}")
    logger(_format_target_years_normalization_log(config, target_years))

    resolved_sources = _resolve_sources(config.source)
    source_order = {"jma": 0, "water_info": 1}
    jma_stations: list[JMAStationInput] = []
    waterinfo_codes: list[str] = []

    if "jma" in resolved_sources:
        jma_stations = _resolve_jma_stations_for_config(config, logger)
    if "water_info" in resolved_sources:
        waterinfo_codes = _resolve_waterinfo_codes_for_config(config, logger)

    if not jma_stations and not waterinfo_codes:
        dataset = RainfallDataset(records=[], errors=["No stations resolved"])
        empty = pd.DataFrame()
        return RainfallAnalyzeResult(dataset=dataset, timeseries_df=empty, annual_max_df=empty, excel_paths=[], chart_paths=[])

    station_name_by_key: dict[tuple[str, str], str] = {}
    jma_station_inputs: dict[str, list[JMAStationInput]] = {}
    waterinfo_station_inputs: dict[str, list[WaterInfoStationInput]] = {}
    job_units: list[tuple[str, str, int]] = []

    grouped_jma_stations: dict[str, list[JMAStationInput]] = {}
    for station in jma_stations:
        grouped_jma_stations.setdefault(station.station_key, []).append(station)

    for station_key, station_group in sorted(grouped_jma_stations.items()):
        if _is_cancelled(should_stop):
            _append_cancelled_once(all_errors)
            break
        primary_station = station_group[0]
        station_name = next((station.station_name for station in station_group if station.station_name), "")
        station_name_by_key[("jma", station_key)] = station_name
        jma_station_inputs[station_key] = list(station_group)

        years_for_station = target_years
        station_period_reason = "JMA観測所別の年判定前（全体対象年を仮適用）"
        if target_years:
            requested_count = len(target_years)
            filtered_years = target_years
            availability = fetch_available_years_hourly(
                prec_no=primary_station.prefecture_code,
                block_no=primary_station.block_number,
            )
            if availability.status == "indeterminate":
                logger(
                    f"[JMA][availability] 観測所={station_key} 指定年数={requested_count}"
                    f" -> 判定後年数={requested_count} status={availability.status}"
                )
                logger(
                    f"[JMA][availability] 観測所={station_key} "
                    f"status=indeterminate ({availability.reason}) 従来モードで継続"
                )
                station_period_reason = (
                    f"可用性判定が不確定のため全体対象年を使用 "
                    f"(status={availability.status}, reason={availability.reason})"
                )
            else:
                filtered_years = [year for year in target_years if year in availability.years]
                logger(
                    f"[JMA][availability] 観測所={station_key} 指定年数={requested_count}"
                    f" -> 判定後年数={len(filtered_years)} status={availability.status}"
                )
                if not filtered_years:
                    logger(f"[JMA][availability] 観測所={station_key} 判定後の対象年なしのためスキップ")
                    station_period_reason = "可用性判定の結果、対象年なし"
                else:
                    station_period_reason = "可用性判定で存在年のみへ絞り込み"

            jma_requested_year_total += requested_count
            jma_filtered_year_total += len(filtered_years)
            years_for_station = filtered_years
        logger(_format_station_target_period_log("jma", station_key, years_for_station, station_period_reason))

        for year in years_for_station:
            if _is_cancelled(should_stop):
                _append_cancelled_once(all_errors)
                break
            job_units.append(("jma", station_key, year))

    for code in waterinfo_codes:
        if _is_cancelled(should_stop):
            _append_cancelled_once(all_errors)
            break
        station_name_by_key[("water_info", code)] = ""
        waterinfo_station_inputs[code] = [WaterInfoStationInput(station_code=code)]
        logger(
            _format_station_target_period_log(
                "water_info",
                code,
                target_years,
                "WaterInfoは観測所別の年可用性判定を行わないため、全体対象年をそのまま使用",
            )
        )
        for year in target_years:
            if _is_cancelled(should_stop):
                _append_cancelled_once(all_errors)
                break
            job_units.append(("water_info", code, year))

    if config.collection_order == "year_station":
        job_units.sort(key=lambda item: (item[2], source_order.get(item[0], 99), item[1]))
    else:
        job_units.sort(key=lambda item: (source_order.get(item[0], 99), item[1], item[2]))

    out_dir_path = Path(output_dir)
    out_dir_path.mkdir(parents=True, exist_ok=True)

    for source_type, station_key, year in job_units:
        if _is_cancelled(should_stop):
            _append_cancelled_once(all_errors)
            break

        station_name = station_name_by_key.get((source_type, station_key), "")
        if source_type == "jma":
            source_df = _fetch_jma_year_monthly(
                station_obj_list=jma_station_inputs.get(station_key, []),
                station_key=station_key,
                year=year,
                output_dir=output_dir,
                config=config,
                logger=logger,
                should_stop=should_stop,
                all_errors=all_errors,
                records_counter=lambda n: None,
                created_parquet_paths=created_parquet_paths,
            )
        else:
            source_df = _fetch_waterinfo_year(
                station_obj_list=waterinfo_station_inputs.get(station_key, []),
                station_key=station_key,
                year=year,
                output_dir=output_dir,
                config=config,
                logger=logger,
                should_stop=should_stop,
                all_errors=all_errors,
                created_parquet_paths=created_parquet_paths,
            )

        if source_df is None or source_df.empty:
            continue

        station_actual_name = station_name
        if not station_actual_name:
            station_actual_name = source_df["station_name"].iloc[0] if "station_name" in source_df.columns else ""

        timeseries_df = build_hourly_timeseries_dataframe(source_df)
        annual_max_df = build_annual_max_dataframe(timeseries_df)
        del source_df

        if _is_cancelled(should_stop):
            _append_cancelled_once(all_errors)
            del timeseries_df, annual_max_df
            break

        if export_excel and not timeseries_df.empty:
            safe_name = str(station_actual_name).replace("/", "_").replace("\\", "_")
            filename = f"{station_key}_{safe_name}.xlsx" if safe_name else f"{station_key}.xlsx"
            excel_dir = out_dir_path / "excel"
            excel_dir.mkdir(parents=True, exist_ok=True)
            output_path = excel_dir / filename

            path = export_station_rainfall_excel(
                timeseries_df,
                annual_max_df,
                output_path=str(output_path),
                decimal_places=decimal_places,
            )
            if path is not None:
                excel_paths.add(str(path))

        if _is_cancelled(should_stop):
            _append_cancelled_once(all_errors)
            del timeseries_df, annual_max_df
            break

        if export_chart and not timeseries_df.empty and not annual_max_df.empty:
            generated = export_rainfall_charts(
                timeseries_df,
                annual_max_df,
                output_dir=str(out_dir_path / "charts"),
                station_key=station_key,
                station_name=station_actual_name,
                should_stop=should_stop,
            )
            chart_paths.extend(str(p) for p in generated)
            if _is_cancelled(should_stop):
                _append_cancelled_once(all_errors)
                del timeseries_df, annual_max_df
                break

        del timeseries_df, annual_max_df

    if jma_requested_year_total > 0:
        reduced = jma_requested_year_total - jma_filtered_year_total
        logger(
            "[JMA][availability] 全体年判定: "
            f"{jma_requested_year_total} -> {jma_filtered_year_total} ({reduced} 年削減)"
        )

    if "cancelled" in all_errors:
        kept_count = len(dict.fromkeys(created_parquet_paths))
        if kept_count > 0:
            logger(f"[Parquet] 停止時も {kept_count} 件の新規Parquetを保持します。")

    dataset = RainfallDataset(records=[], errors=all_errors)
    empty = pd.DataFrame()
    return RainfallAnalyzeResult(
        dataset=dataset,
        timeseries_df=empty,
        annual_max_df=empty,
        excel_paths=sorted(list(excel_paths)),
        chart_paths=sorted(chart_paths),
    )


def _fetch_jma_year_monthly(
    *,
    station_obj_list: list[JMAStationInput],
    station_key: str,
    year: int,
    output_dir: str,
    config: RainfallRunInput,
    logger: LogFn,
    should_stop: CancelFn | None,
    all_errors: list[str],
    records_counter: Callable[[int], None],
    created_parquet_paths: list[Path],
) -> pd.DataFrame | None:
    return _rainfall_fetch_jma_service.fetch_jma_year_monthly(
        station_obj_list=station_obj_list,
        station_key=station_key,
        year=year,
        output_dir=output_dir,
        config=config,
        logger=logger,
        should_stop=should_stop,
        all_errors=all_errors,
        records_counter=records_counter,
        created_parquet_paths=created_parquet_paths,
    )


def _fetch_waterinfo_year(
    *,
    station_obj_list: list[WaterInfoStationInput],
    station_key: str,
    year: int,
    output_dir: str,
    config: RainfallRunInput,
    logger: LogFn,
    should_stop: CancelFn | None,
    all_errors: list[str],
    created_parquet_paths: list[Path],
) -> pd.DataFrame | None:
    return _rainfall_fetch_waterinfo_service.fetch_waterinfo_year(
        station_obj_list=station_obj_list,
        station_key=station_key,
        year=year,
        output_dir=output_dir,
        config=config,
        logger=logger,
        should_stop=should_stop,
        all_errors=all_errors,
        created_parquet_paths=created_parquet_paths,
    )
