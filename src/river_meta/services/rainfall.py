from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Callable

from pathlib import Path

from river_meta.rainfall.analysis import build_annual_max_dataframe, build_hourly_timeseries_dataframe
from river_meta.rainfall.chart_exporter import export_rainfall_charts
from river_meta.rainfall.excel_exporter import export_station_rainfall_excel
from river_meta.rainfall.generate_manifest import (
    build_chart_id,
    build_digest_from_parquet_paths,
    build_station_digest,
    build_station_id,
    load_manifest,
    save_manifest,
)
from river_meta.rainfall.parquet_store import (
    ParquetEntry,
    build_parquet_path,
    find_missing_months,
    load_and_concat_monthly_parquets,
    load_records_parquet,
    migrate_legacy_jma_parquets,
    parquet_exists,
    save_records_parquet,
    scan_parquet_dir,
)
from river_meta.rainfall.jma_adapter import fetch_jma_rainfall
from river_meta.rainfall.jma_availability import fetch_available_years_hourly
from river_meta.rainfall.models import (
    JMAStationInput,
    RainfallDataset,
    RainfallQuery,
    WaterInfoStationInput,
)
from river_meta.rainfall.normalizer import normalize_interval_token, normalize_source_token
from river_meta.rainfall.station_index import (
    resolve_jma_stations_from_codes,
    resolve_jma_stations_from_prefectures,
)
from river_meta.rainfall.waterinfo_station_index import resolve_waterinfo_station_codes_from_prefectures
from river_meta.rainfall.waterinfo_adapter import fetch_waterinfo_rainfall

if TYPE_CHECKING:
    import pandas as pd


LogFn = Callable[[str], None]
CancelFn = Callable[[], bool]


@dataclass(slots=True)
class RainfallRunInput:
    source: str
    start_at: datetime | None = None
    end_at: datetime | None = None
    year: int | None = None
    years: list[int] | None = None
    interval: str = "1hour"
    jma_prefectures: list[str] = field(default_factory=list)
    jma_station_codes: list[str] = field(default_factory=list)
    jma_stations: list[tuple[str, str, str]] = field(default_factory=list)
    waterinfo_prefectures: list[str] = field(default_factory=list)
    waterinfo_station_codes: list[str] = field(default_factory=list)
    jma_station_index_path: str | None = None
    jma_log_level: str | None = None
    jma_enable_log_output: bool | None = None
    include_raw: bool = False


@dataclass(slots=True)
class RainfallAnalyzeResult:
    dataset: RainfallDataset
    timeseries_df: "pd.DataFrame"
    annual_max_df: "pd.DataFrame"
    excel_paths: list[str] = field(default_factory=list)
    chart_paths: list[str] = field(default_factory=list)


@dataclass(slots=True)
class RainfallGenerateInput:
    """Parquet ディレクトリを入力として Excel/グラフを生成するための設定。"""
    parquet_dir: str
    export_excel: bool = True
    export_chart: bool = True
    decimal_places: int = 2
    target_stations: list[tuple[str, str]] = field(default_factory=list)
    use_diff_mode: bool = True
    force_full_regenerate: bool = False


@dataclass(slots=True)
class RainfallGenerateResult:
    """generate モードの出力結果。"""
    entries: list[ParquetEntry] = field(default_factory=list)
    incomplete_entries: list[ParquetEntry] = field(default_factory=list)
    excel_paths: list[str] = field(default_factory=list)
    chart_paths: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def _noop_log(_: str) -> None:
    return None


def _append_cancelled_once(errors: list[str]) -> None:
    if "cancelled" not in errors:
        errors.append("cancelled")


def _rollback_created_parquets(paths: list[Path], logger: LogFn) -> None:
    if not paths:
        return
    # 重複排除しつつ順序は維持
    unique_paths = list(dict.fromkeys(paths))
    removed = 0
    for path in unique_paths:
        try:
            if path.exists():
                path.unlink()
                removed += 1
        except Exception as exc:  # noqa: BLE001
            logger(f"[Parquet][WARN] ロールバック削除失敗: {path.name} ({type(exc).__name__}: {exc})")
    if removed:
        logger(f"[Parquet] 停止により {removed} 件の新規Parquetをロールバック削除しました。")


def _collect_parquet_paths_for_entry(output_dir: str | Path, entry: ParquetEntry) -> list[Path]:
    if entry.source == "jma":
        return [
            build_parquet_path(output_dir, entry.source, entry.station_key, entry.year, month=month)
            for month in range(1, 13)
        ]
    return [build_parquet_path(output_dir, entry.source, entry.station_key, entry.year)]


def _sanitize_path_token(value: str) -> str:
    return str(value).replace("/", "_").replace("\\", "_")


def _build_excel_output_path(output_dir: str | Path, station_key: str, station_name: str) -> Path:
    safe_name = _sanitize_path_token(station_name)
    filename = f"{station_key}_{safe_name}.xlsx" if safe_name else f"{station_key}.xlsx"
    return Path(output_dir) / "excel" / filename


def _build_chart_output_path(
    output_dir: str | Path,
    station_key: str,
    station_name: str,
    year: int,
    metric: str,
) -> Path:
    safe_station_name = _sanitize_path_token(station_name) if station_name else ""
    safe_station_key = _sanitize_path_token(station_key)
    subdir_name = f"{safe_station_name}_{safe_station_key}" if safe_station_name else safe_station_key
    safe_metric = _sanitize_path_token(metric)
    return Path(output_dir) / "charts" / subdir_name / f"{year}_{safe_metric}.png"


def _to_relpath(path: Path, root: Path) -> str:
    try:
        return path.resolve().relative_to(root.resolve()).as_posix()
    except Exception:
        return path.as_posix()


def run_rainfall_generate(
    config: RainfallGenerateInput,
    *,
    log: LogFn | None = None,
    should_stop: CancelFn | None = None,
) -> RainfallGenerateResult:
    """Parquet ディレクトリを入力として Excel/グラフを生成する。

    完全年（JMAなら12ヶ月分のParquetが揃っている年）のみ出力する。
    不完全年は ``incomplete_entries`` に格納してスキップする。
    """
    import pandas as pd

    logger = log or _noop_log
    diff_mode = bool(config.use_diff_mode) and not bool(config.force_full_regenerate)
    all_errors: list[str] = []
    excel_paths: list[str] = []
    chart_paths: list[str] = []
    generated_excel_count = 0
    skipped_excel_count = 0
    generated_chart_count = 0
    skipped_chart_count = 0
    logger(
        "[generate] mode="
        f"{'diff' if diff_mode else 'full'} "
        f"(use_diff_mode={config.use_diff_mode}, force_full_regenerate={config.force_full_regenerate})"
    )

    entries = scan_parquet_dir(config.parquet_dir)
    if not entries:
        logger("[generate] Parquet ファイルが見つかりません。")
        return RainfallGenerateResult(errors=["No parquet files found"])

    target_set = {
        (str(source), str(station_key))
        for source, station_key in config.target_stations
        if str(source).strip() and str(station_key).strip()
    }
    if target_set:
        entries = [entry for entry in entries if (entry.source, entry.station_key) in target_set]
        if not entries:
            logger("[generate] 指定された観測所に一致する Parquet が見つかりません。")
            return RainfallGenerateResult(errors=["No parquet files for selected stations"])

    complete_entries: list[ParquetEntry] = []
    incomplete_entries: list[ParquetEntry] = []

    for entry in entries:
        if _is_cancelled(should_stop):
            _append_cancelled_once(all_errors)
            break
        if entry.complete:
            complete_entries.append(entry)
        else:
            missing = find_missing_months(
                config.parquet_dir, entry.source, entry.station_key, entry.year,
            )
            logger(
                f"[generate][WARN] 観測所={entry.station_key} 年={entry.year}"
                f" 不足月={missing} — スキップ"
            )
            incomplete_entries.append(entry)

    if not complete_entries:
        if "cancelled" in all_errors:
            logger("[generate] 停止要求により処理を中断しました。")
            return RainfallGenerateResult(
                entries=entries,
                incomplete_entries=incomplete_entries,
                errors=all_errors,
            )
        logger("[generate] 完全年のデータがありません。")
        return RainfallGenerateResult(
            entries=entries,
            incomplete_entries=incomplete_entries,
            errors=["No complete year data found"],
        )

    out_dir_path = Path(config.parquet_dir)
    manifest_path = out_dir_path / "metadata" / "excel_manifest.json"
    manifest = load_manifest(manifest_path, log=logger)
    excel_manifest = manifest.get("excel")
    chart_manifest = manifest.get("charts")
    if not isinstance(excel_manifest, dict):
        excel_manifest = {}
        manifest["excel"] = excel_manifest
    if not isinstance(chart_manifest, dict):
        chart_manifest = {}
        manifest["charts"] = chart_manifest
    manifest_dirty = False

    grouped_entries: dict[tuple[str, str], list[ParquetEntry]] = {}
    for entry in complete_entries:
        grouped_entries.setdefault((entry.source, entry.station_key), []).append(entry)

    for (source, station_key), station_entries in sorted(grouped_entries.items(), key=lambda item: item[0]):
        if _is_cancelled(should_stop):
            _append_cancelled_once(all_errors)
            break

        station_entries = sorted(station_entries, key=lambda item: item.year)
        station_id = build_station_id(source, station_key)
        year_digests: dict[int, str] = {}
        for entry in station_entries:
            parquet_paths = _collect_parquet_paths_for_entry(config.parquet_dir, entry)
            year_digests[entry.year] = build_digest_from_parquet_paths(config.parquet_dir, parquet_paths)
        station_digest = build_station_digest(year_digests)

        prev_excel_record = excel_manifest.get(station_id)
        if not isinstance(prev_excel_record, dict):
            prev_excel_record = {}
        prev_station_digest = str(prev_excel_record.get("station_digest", "")).strip()
        prev_excel_relpath = str(prev_excel_record.get("output_relpath", "")).strip()
        excel_skip_by_digest = (
            diff_mode
            and config.export_excel
            and prev_station_digest
            and prev_station_digest == station_digest
            and prev_excel_relpath
            and (out_dir_path / prev_excel_relpath).exists()
        )

        if excel_skip_by_digest and not config.export_chart:
            logger(f"[generate] 観測所={station_key} は digest 一致のため Excel をスキップ")
            skipped_excel_count += 1
            continue

        source_dfs: list[pd.DataFrame] = []
        station_name = ""
        years_text = ",".join(str(item.year) for item in station_entries)
        logger(f"[generate] 処理中: {source} 観測所={station_key} 年={years_text}")

        for entry in station_entries:
            if _is_cancelled(should_stop):
                _append_cancelled_once(all_errors)
                break

            # Parquet 読み込み（観測所単位で年をまとめる）
            if entry.source == "jma":
                source_df = load_and_concat_monthly_parquets(
                    config.parquet_dir, entry.source, entry.station_key, entry.year,
                )
            else:
                pq_path = build_parquet_path(
                    config.parquet_dir, entry.source, entry.station_key, entry.year,
                )
                source_df = load_records_parquet(pq_path)

            if source_df is None or source_df.empty:
                continue

            if not station_name and "station_name" in source_df.columns:
                station_name = str(source_df["station_name"].iloc[0])
            source_dfs.append(source_df)

        if _is_cancelled(should_stop):
            _append_cancelled_once(all_errors)
            break

        if not source_dfs:
            continue

        source_df = pd.concat(source_dfs, ignore_index=True) if len(source_dfs) > 1 else source_dfs[0]
        del source_dfs

        timeseries_df = build_hourly_timeseries_dataframe(source_df)
        annual_max_df = build_annual_max_dataframe(timeseries_df)
        del source_df

        if _is_cancelled(should_stop):
            _append_cancelled_once(all_errors)
            del timeseries_df, annual_max_df
            break

        if config.export_excel and not timeseries_df.empty:
            output_path = _build_excel_output_path(config.parquet_dir, station_key, station_name)
            if excel_skip_by_digest:
                logger(f"[generate] 観測所={station_key} は digest 一致のため Excel をスキップ")
                skipped_excel_count += 1
            else:
                path = export_station_rainfall_excel(
                    timeseries_df,
                    annual_max_df,
                    output_path=str(output_path),
                    decimal_places=config.decimal_places,
                )
                if path is not None:
                    excel_paths.append(str(path))
                    generated_excel_count += 1
                    excel_manifest[station_id] = {
                        "station_digest": station_digest,
                        "output_relpath": _to_relpath(path, out_dir_path),
                        "year_digests": {str(year): digest for year, digest in sorted(year_digests.items())},
                    }
                    manifest_dirty = True

        if _is_cancelled(should_stop):
            _append_cancelled_once(all_errors)
            del timeseries_df, annual_max_df
            break

        if config.export_chart and not timeseries_df.empty and not annual_max_df.empty:
            chart_target = annual_max_df
            if diff_mode:
                target_indices: list[int] = []
                for idx, row in annual_max_df.iterrows():
                    year = int(row["年"])
                    metric = str(row["指標"])
                    chart_id = build_chart_id(source, station_key, year, metric)
                    prev_chart_record = chart_manifest.get(chart_id)
                    if not isinstance(prev_chart_record, dict):
                        target_indices.append(idx)
                        continue
                    prev_year_digest = str(prev_chart_record.get("year_digest", "")).strip()
                    prev_chart_relpath = str(prev_chart_record.get("output_relpath", "")).strip()
                    if (
                        prev_year_digest
                        and prev_year_digest == year_digests.get(year, "")
                        and prev_chart_relpath
                        and (out_dir_path / prev_chart_relpath).exists()
                    ):
                        skipped_chart_count += 1
                        continue
                    target_indices.append(idx)
                chart_target = annual_max_df.loc[target_indices].copy() if target_indices else annual_max_df.iloc[0:0]

            if chart_target.empty and diff_mode:
                logger(f"[generate] 観測所={station_key} のグラフは digest 一致のため全件スキップ")

            generated = export_rainfall_charts(
                timeseries_df,
                chart_target,
                output_dir=str(out_dir_path / "charts"),
                station_key=station_key,
                station_name=station_name,
                should_stop=should_stop,
            )
            chart_paths.extend(str(p) for p in generated)
            generated_chart_count += len(generated)
            if _is_cancelled(should_stop):
                _append_cancelled_once(all_errors)
                del timeseries_df, annual_max_df
                break
            for _, row in chart_target.iterrows():
                year = int(row["年"])
                metric = str(row["指標"])
                expected_output = _build_chart_output_path(
                    config.parquet_dir,
                    station_key,
                    station_name,
                    year,
                    metric,
                )
                if not expected_output.exists():
                    continue
                chart_id = build_chart_id(source, station_key, year, metric)
                chart_manifest[chart_id] = {
                    "year_digest": year_digests.get(year, ""),
                    "output_relpath": _to_relpath(expected_output, out_dir_path),
                }
                manifest_dirty = True

        del timeseries_df, annual_max_df

    if manifest_dirty:
        try:
            save_manifest(manifest_path, manifest)
        except Exception as exc:  # noqa: BLE001
            logger(f"[generate][WARN] manifest 保存失敗: {type(exc).__name__}: {exc}")

    logger(
        "[generate] summary "
        f"excel(generated={generated_excel_count}, skipped={skipped_excel_count}) "
        f"chart(generated={generated_chart_count}, skipped={skipped_chart_count})"
    )

    return RainfallGenerateResult(
        entries=entries,
        incomplete_entries=incomplete_entries,
        excel_paths=sorted(excel_paths),
        chart_paths=sorted(chart_paths),
        errors=all_errors,
    )

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

    all_records_count = 0
    all_errors: list[str] = []
    excel_paths: set[str] = set()
    chart_paths: list[str] = []
    years = config.years if config.years else ([config.year] if config.year else [])
    years = list(dict.fromkeys(years))
    created_parquet_paths: list[Path] = []
    jma_requested_year_total = 0
    jma_filtered_year_total = 0

    # We first collect the fully resolved stations to iterate over them safely without querying API twice
    resolved_sources = _resolve_sources(config.source)
    jma_stations: list[JMAStationInput] = []
    waterinfo_codes: list[str] = []

    if "jma" in resolved_sources:
        jma_stations = _resolve_jma_stations_for_config(config, logger)
    if "water_info" in resolved_sources:
        waterinfo_codes = _resolve_waterinfo_codes_for_config(config, logger)

    stations_to_process = []
    for s in jma_stations:
        stations_to_process.append(("jma", s.station_key, s.station_name, [s]))
    for code in waterinfo_codes:
        stations_to_process.append(("water_info", code, "", [WaterInfoStationInput(station_code=code)]))

    if not stations_to_process:
        dataset = RainfallDataset(records=[], errors=["No stations resolved"])
        empty = pd.DataFrame()
        return RainfallAnalyzeResult(dataset=dataset, timeseries_df=empty, annual_max_df=empty, excel_paths=[], chart_paths=[])

    out_dir_path = Path(output_dir)
    out_dir_path.mkdir(parents=True, exist_ok=True)

    for source_type, station_key, station_name, station_obj_list in stations_to_process:
        if _is_cancelled(should_stop):
            _append_cancelled_once(all_errors)
            break

        years_for_station = years
        if source_type == "jma" and years:
            station = station_obj_list[0]
            requested_count = len(years)
            filtered_years = years
            availability = fetch_available_years_hourly(
                prec_no=station.prefecture_code,
                block_no=station.block_number,
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
            else:
                filtered_years = [year for year in years if year in availability.years]
                logger(
                    f"[JMA][availability] 観測所={station_key} 指定年数={requested_count}"
                    f" -> 判定後年数={len(filtered_years)} status={availability.status}"
                )
                if not filtered_years:
                    logger(f"[JMA][availability] 観測所={station_key} 判定後の対象年なしのためスキップ")

            jma_requested_year_total += requested_count
            jma_filtered_year_total += len(filtered_years)
            years_for_station = filtered_years

        for year in years_for_station:
            if _is_cancelled(should_stop):
                _append_cancelled_once(all_errors)
                break

            # --- JMA: 月単位で取得・キャッシュ ---
            if source_type == "jma":
                source_df = _fetch_jma_year_monthly(
                    station_obj_list=station_obj_list,
                    station_key=station_key,
                    year=year,
                    output_dir=output_dir,
                    config=config,
                    logger=logger,
                    should_stop=should_stop,
                    all_errors=all_errors,
                    records_counter=lambda n: None,  # noqa: ARG005
                    created_parquet_paths=created_parquet_paths,
                )
            else:
                # --- water_info: 年単位で取得・キャッシュ ---
                source_df = _fetch_waterinfo_year(
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

            if source_df is None or source_df.empty:
                continue

            station_actual_name = station_name
            if not station_actual_name:
                station_actual_name = source_df["station_name"].iloc[0] if "station_name" in source_df.columns else ""

            timeseries_df = build_hourly_timeseries_dataframe(source_df)
            annual_max_df = build_annual_max_dataframe(timeseries_df)
            del source_df  # メモリ解放

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

            del timeseries_df, annual_max_df  # メモリ解放

    if jma_requested_year_total > 0:
        reduced = jma_requested_year_total - jma_filtered_year_total
        logger(
            "[JMA][availability] 全体年判定: "
            f"{jma_requested_year_total} -> {jma_filtered_year_total} ({reduced} 年削減)"
        )

    if "cancelled" in all_errors:
        _rollback_created_parquets(created_parquet_paths, logger)

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
    """JMA の1年分データを月単位で取得・キャッシュし、結合して返す。"""
    import calendar
    import pandas as pd

    # 旧フォーマット (block_number のみ) のファイルがあればリネーム
    block_number = station_obj_list[0].block_number if station_obj_list else ""
    if block_number and block_number != station_key:
        migrated = migrate_legacy_jma_parquets(output_dir, block_number, station_key, year)
        if migrated:
            logger(f"[Parquet] 旧フォーマットから {migrated} ファイルをリネームしました (観測所={station_key}, 年={year})")

    any_fetched = False

    for month in range(1, 13):
        if _is_cancelled(should_stop):
            _append_cancelled_once(all_errors)
            break

        if parquet_exists(output_dir, "jma", station_key, year, month=month):
            logger(f"[JMA] 観測所={station_key} {year}/{month:02d} キャッシュあり")
            continue

        # この月のデータを取得
        last_day = calendar.monthrange(year, month)[1]
        query_start = datetime(year, month, 1, 0, 0, 0)
        query_end = datetime(year, month, last_day, 23, 59, 59)
        query = RainfallQuery(start_at=query_start, end_at=query_end, interval="1hour")

        logger(f"[JMA] 観測所={station_key} {year}/{month:02d} データ取得中...")
        part = _collect_jma_with_resolved(
            station_obj_list, query, config.include_raw, logger, should_stop,
            config.jma_log_level, config.jma_enable_log_output,
        )
        all_errors.extend(part.errors)
        if _is_cancelled(should_stop) or "cancelled" in part.errors:
            _append_cancelled_once(all_errors)
            break

        if not part.records:
            logger(f"[JMA] 観測所={station_key} {year}/{month:02d} データなし")
            continue

        logger(f"[JMA] 観測所={station_key} {year}/{month:02d} 取得完了 ({len(part.records)}件)")
        any_fetched = True
        records_counter(len(part.records))

        pq_path = build_parquet_path(output_dir, "jma", station_key, year, month=month)
        save_records_parquet(part.records, pq_path)
        created_parquet_paths.append(pq_path)
        logger(f"[Parquet] 保存完了: {pq_path.name}")
        del part

    # 12ヶ月分を結合
    combined = load_and_concat_monthly_parquets(output_dir, "jma", station_key, year)
    if combined.empty:
        return None
    return combined


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
    """water_info の1年分データを年単位で取得・キャッシュして返す。"""

    if _is_cancelled(should_stop):
        _append_cancelled_once(all_errors)
        return None

    pq_path = build_parquet_path(output_dir, "water_info", station_key, year)

    if parquet_exists(output_dir, "water_info", station_key, year):
        logger(f"[水文水質DB] 観測所={station_key} 年={year} キャッシュから読み込み")
        return load_records_parquet(pq_path)

    logger(f"[水文水質DB] 観測所={station_key} 年={year} データ取得中...")
    query_start = datetime(year, 1, 1, 0, 0, 0)
    query_end = datetime(year, 12, 31, 23, 59, 59)
    query = RainfallQuery(start_at=query_start, end_at=query_end, interval="1hour")

    part = _collect_waterinfo_with_resolved(
        station_obj_list, query, config.include_raw, logger, should_stop,
    )
    all_errors.extend(part.errors)
    if _is_cancelled(should_stop) or "cancelled" in part.errors:
        _append_cancelled_once(all_errors)
        return None

    if not part.records:
        logger(f"[水文水質DB] 観測所={station_key} 年={year} データなし")
        return None

    valid_rainfall_count = sum(1 for record in part.records if record.rainfall_mm is not None)
    if valid_rainfall_count == 0:
        logger(f"[水文水質DB] 観測所={station_key} 年={year} 有効値なしのため保存スキップ")
        return None

    logger(f"[水文水質DB] 観測所={station_key} 年={year} 取得完了 ({len(part.records)}件)")
    save_records_parquet(part.records, pq_path)
    created_parquet_paths.append(pq_path)
    logger(f"[Parquet] 保存完了: {pq_path.name}")

    source_df = part.to_dataframe()
    del part
    return source_df

def _dedupe_jma_stations(stations: list[JMAStationInput]) -> list[JMAStationInput]:
    deduped: list[JMAStationInput] = []
    seen: set[tuple[str, str, str]] = set()
    for station in stations:
        key = (station.prefecture_code, station.block_number, station.obs_type)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(station)
    return deduped


def _dedupe_codes(codes: list[str]) -> list[str]:
    values = sorted(set(codes), key=lambda value: (0, int(value)) if value.isdigit() else (1, value))
    return values


def _resolve_sources(source: str) -> list[str]:
    token = str(source or "").strip().lower()
    if token in {"both", "all", "jma+water_info", "water_info+jma", "jma+waterinfo", "waterinfo+jma"}:
        return ["jma", "water_info"]
    return [normalize_source_token(source)]


def _resolve_jma_stations_for_config(config: RainfallRunInput, logger: LogFn) -> list[JMAStationInput]:
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
        from_prefs, pref_issues = resolve_jma_stations_from_prefectures(
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
        resolved, issues = resolve_jma_stations_from_codes(
            config.jma_station_codes,
            index_path=config.jma_station_index_path,
        )
        stations.extend(resolved)
        for issue in issues:
            logger(f"station_code={issue.code} resolve_error={issue.reason}")
    stations = _dedupe_jma_stations(stations)
    return stations


def _resolve_waterinfo_codes_for_config(config: RainfallRunInput, logger: LogFn) -> list[str]:
    station_codes = [str(code).strip() for code in config.waterinfo_station_codes if str(code).strip()]
    if config.waterinfo_prefectures:
        pref_codes, pref_issues = resolve_waterinfo_station_codes_from_prefectures(
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
    station_codes = _dedupe_codes(station_codes)
    return station_codes


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
        return _collect_jma_with_resolved(stations, query, config.include_raw, logger, should_stop, config.jma_log_level, config.jma_enable_log_output)
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
    try:
        records = fetch_jma_rainfall(
            stations=stations,
            query=query,
            include_raw=include_raw,
            log_warn=logger,
            should_stop=should_stop,
            jma_log_level=jma_log_level,
            jma_enable_log_output=jma_enable_log_output,
        )
        return RainfallDataset(records=records, errors=[])
    except Exception as exc:  # noqa: BLE001
        message = f"jma:{type(exc).__name__}: {exc}"
        logger(message)
        return RainfallDataset(records=[], errors=[message])


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
    try:
        records = fetch_waterinfo_rainfall(
            stations=stations,
            query=query,
            include_raw=include_raw,
            log_warn=logger,
            should_stop=should_stop,
        )
        return RainfallDataset(records=records, errors=[])
    except Exception as exc:  # noqa: BLE001
        message = f"water_info:{type(exc).__name__}: {exc}"
        logger(message)
        return RainfallDataset(records=[], errors=[message])


def _resolve_query_period(config: RainfallRunInput) -> tuple[datetime, datetime]:
    if config.start_at is not None and config.end_at is not None:
        if config.year is not None or config.years:
            raise ValueError("Specify either year/years or start_at/end_at, not both")
        return config.start_at, config.end_at

    years = config.years if config.years else ([config.year] if config.year else [])
    if years:
        start_year = min(years)
        end_year = max(years)
        if start_year < 1900 or end_year > 2100:
            raise ValueError(f"Unsupported year range: {start_year}-{end_year}")
        return datetime(start_year, 1, 1, 0, 0, 0), datetime(end_year, 12, 31, 23, 59, 59)

    if config.start_at is None and config.end_at is None:
        raise ValueError("start_at/end_at or year(s) is required")
    raise ValueError("Both start_at and end_at are required when year is not specified")


def _is_cancelled(should_stop: CancelFn | None) -> bool:
    if should_stop is None:
        return False
    try:
        return bool(should_stop())
    except Exception:
        return False
