from __future__ import annotations

import shutil
from concurrent.futures import ProcessPoolExecutor, as_completed
from concurrent.futures.process import BrokenProcessPool
from pathlib import Path

from river_meta.rainfall.outputs.analysis import build_annual_max_dataframe, build_hourly_timeseries_dataframe
from river_meta.rainfall.outputs.chart_exporter import export_rainfall_charts
from river_meta.rainfall.outputs.excel_exporter import export_station_rainfall_excel
from river_meta.rainfall.storage.manifest import (
    build_chart_id,
    build_digest_from_parquet_paths,
    build_station_digest,
    build_station_id,
    load_manifest,
    save_manifest,
)
from river_meta.rainfall.storage.parquet_store import ParquetEntry, find_missing_months, scan_parquet_dir
from river_meta.rainfall.support.common import (
    CancelFn,
    LogFn,
    append_cancelled_once as _append_cancelled_once,
    build_excel_output_path as _build_excel_output_path,
    is_cancelled as _is_cancelled,
    noop_log as _noop_log,
    to_relpath as _to_relpath,
)
from river_meta.rainfall.support.generate_support import (
    ChartGenerateJob as _ChartGenerateJob,
    ExcelGenerateJob as _ExcelGenerateJob,
    ExcelGenerateJobResult as _ExcelGenerateJobResult,
    build_chart_targets as _build_chart_targets,
    collect_parquet_paths_for_entry as _collect_parquet_paths_for_entry,
    load_source_dataframe_for_station_entries as _load_source_dataframe_for_station_entries,
    run_chart_generate_job as _run_chart_generate_job,
    run_excel_generate_job as _run_excel_generate_job,
    update_chart_manifest_entries as _update_chart_manifest_entries,
)

from ..domain.usecase_models import RainfallGenerateInput, RainfallGenerateResult


def _remove_file_if_exists(path: Path) -> bool:
    if not path.exists() or not path.is_file():
        return False
    path.unlink()
    return True


def _remove_dir_if_exists(path: Path) -> bool:
    if not path.exists() or not path.is_dir():
        return False
    shutil.rmtree(path)
    return True


def _matches_station_excel_filename(path: Path, station_key: str) -> bool:
    stem = path.stem
    return stem == station_key or stem.startswith(f"{station_key}_")


def _cleanup_generate_outputs_for_full_mode(
    *,
    out_dir_path: Path,
    manifest_path: Path,
    target_set: set[tuple[str, str]],
    logger: LogFn,
) -> dict[str, object]:
    if not target_set:
        removed_excel_dir = _remove_dir_if_exists(out_dir_path / "excel")
        manifest = load_manifest(manifest_path, log=logger)
        manifest["excel"] = {}
        if removed_excel_dir:
            logger("[generate] full mode 初期化: excel を削除")
        else:
            logger("[generate] full mode 初期化: excel の削除対象なし")
        return manifest

    manifest = load_manifest(manifest_path, log=logger)
    excel_manifest = manifest.get("excel")
    chart_manifest = manifest.get("charts")
    if not isinstance(excel_manifest, dict):
        excel_manifest = {}
        manifest["excel"] = excel_manifest
    if not isinstance(chart_manifest, dict):
        chart_manifest = {}
        manifest["charts"] = chart_manifest

    removed_excel = 0
    for source, station_key in sorted(target_set):
        station_id = build_station_id(source, station_key)
        excel_record = excel_manifest.pop(station_id, None)
        if isinstance(excel_record, dict):
            output_relpath = str(excel_record.get("output_relpath", "")).strip()
            if output_relpath and _remove_file_if_exists(out_dir_path / output_relpath):
                removed_excel += 1

        excel_dir = out_dir_path / "excel"
        if excel_dir.exists():
            for candidate in excel_dir.glob("*.xlsx"):
                if _matches_station_excel_filename(candidate, station_key) and _remove_file_if_exists(candidate):
                    removed_excel += 1

    logger("[generate] full mode 初期化: " f"excel={removed_excel}")
    return manifest


def run_rainfall_generate(
    config: RainfallGenerateInput,
    *,
    log: LogFn | None = None,
    should_stop: CancelFn | None = None,
) -> RainfallGenerateResult:
    logger = log or _noop_log
    diff_mode = bool(config.use_diff_mode) and not bool(config.force_full_regenerate)
    excel_parallel_workers = max(1, int(config.excel_parallel_workers))
    excel_parallel_mode = bool(config.excel_parallel_enabled) and excel_parallel_workers > 1
    chart_parallel_workers = max(1, int(config.chart_parallel_workers))
    chart_parallel_mode = bool(config.chart_parallel_enabled) and chart_parallel_workers > 1
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

    out_dir_path = Path(config.parquet_dir)
    manifest_path = out_dir_path / "metadata" / "excel_manifest.json"
    if diff_mode:
        manifest = load_manifest(manifest_path, log=logger)
        manifest_dirty = False
    else:
        manifest = _cleanup_generate_outputs_for_full_mode(
            out_dir_path=out_dir_path,
            manifest_path=manifest_path,
            target_set=target_set,
            logger=logger,
        )
        try:
            save_manifest(manifest_path, manifest)
        except Exception as exc:  # noqa: BLE001
            logger(f"[generate][WARN] full mode 初期化後の manifest 保存失敗: {type(exc).__name__}: {exc}")
        manifest_dirty = False

    complete_entries: list[ParquetEntry] = []
    incomplete_entries: list[ParquetEntry] = []

    for entry in entries:
        if _is_cancelled(should_stop):
            _append_cancelled_once(all_errors)
            break
        if entry.complete:
            complete_entries.append(entry)
        else:
            missing = find_missing_months(config.parquet_dir, entry.source, entry.station_key, entry.year)
            logger(
                f"[generate][WARN] 観測所={entry.station_key} 年={entry.year}"
                f" 不足月={missing} — スキップ"
            )
            incomplete_entries.append(entry)

    if not complete_entries:
        if "cancelled" in all_errors:
            logger("[generate] 停止要求により処理を中断しました。")
            return RainfallGenerateResult(entries=entries, incomplete_entries=incomplete_entries, errors=all_errors)
        logger("[generate] 完全年のデータがありません。")
        return RainfallGenerateResult(
            entries=entries,
            incomplete_entries=incomplete_entries,
            errors=["No complete year data found"],
        )

    excel_manifest = manifest.get("excel")
    chart_manifest = manifest.get("charts")
    if not isinstance(excel_manifest, dict):
        excel_manifest = {}
        manifest["excel"] = excel_manifest
    if not isinstance(chart_manifest, dict):
        chart_manifest = {}
        manifest["charts"] = chart_manifest
    excel_jobs: list[tuple[_ExcelGenerateJob, str, str, dict[int, str]]] = []
    chart_jobs: list[_ChartGenerateJob] = []

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

        station_name = ""
        years_text = ",".join(str(item.year) for item in station_entries)
        logger(f"[generate] 処理中: {source} 観測所={station_key} 年={years_text}")
        if (not config.export_excel or excel_parallel_mode) and (not config.export_chart or chart_parallel_mode):
            if config.export_excel and not excel_skip_by_digest:
                excel_jobs.append(
                    (
                        _ExcelGenerateJob(
                            source=source,
                            station_key=station_key,
                            station_name=station_name,
                            parquet_dir=config.parquet_dir,
                            station_entries=list(station_entries),
                            decimal_places=config.decimal_places,
                        ),
                        station_id,
                        station_digest,
                        dict(year_digests),
                    )
                )
            elif config.export_excel and excel_skip_by_digest:
                logger(f"[generate] 観測所={station_key} は digest 一致のため Excel をスキップ")
                skipped_excel_count += 1

            previous_chart_records: dict[tuple[int, str], tuple[str, str]] = {}
            if config.export_chart and diff_mode:
                for year in sorted(year_digests):
                    for metric in ("1時間雨量", "3時間雨量", "6時間雨量", "12時間雨量", "24時間雨量", "48時間雨量"):
                        chart_id = build_chart_id(source, station_key, year, metric)
                        prev_chart_record = chart_manifest.get(chart_id)
                        if not isinstance(prev_chart_record, dict):
                            continue
                        previous_chart_records[(year, metric)] = (
                            str(prev_chart_record.get("year_digest", "")).strip(),
                            str(prev_chart_record.get("output_relpath", "")).strip(),
                        )
            if config.export_chart:
                chart_jobs.append(
                    _ChartGenerateJob(
                        source=source,
                        station_key=station_key,
                        station_name=station_name,
                        parquet_dir=config.parquet_dir,
                        station_entries=list(station_entries),
                        year_digests=dict(year_digests),
                        chart_targets=None,
                        diff_mode=diff_mode,
                        previous_chart_records=previous_chart_records,
                    )
                )
            continue

        source_df, inferred_station_name = _load_source_dataframe_for_station_entries(config.parquet_dir, station_entries)
        if _is_cancelled(should_stop):
            _append_cancelled_once(all_errors)
            break
        if source_df is None or source_df.empty:
            continue
        station_name = station_name or inferred_station_name

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
            elif excel_parallel_mode:
                excel_jobs.append(
                    (
                        _ExcelGenerateJob(
                            source=source,
                            station_key=station_key,
                            station_name=station_name,
                            parquet_dir=config.parquet_dir,
                            station_entries=list(station_entries),
                            decimal_places=config.decimal_places,
                        ),
                        station_id,
                        station_digest,
                        dict(year_digests),
                    )
                )
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

            chart_targets = _build_chart_targets(chart_target)
            if chart_target.empty and diff_mode:
                logger(f"[generate] 観測所={station_key} のグラフは digest 一致のため全件スキップ")

            if chart_parallel_mode:
                if chart_targets:
                    chart_jobs.append(
                        _ChartGenerateJob(
                            source=source,
                            station_key=station_key,
                            station_name=station_name,
                            parquet_dir=config.parquet_dir,
                            station_entries=list(station_entries),
                            year_digests=dict(year_digests),
                            chart_targets=chart_targets,
                            diff_mode=False,
                            previous_chart_records=None,
                        )
                    )
            else:
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
                if _update_chart_manifest_entries(
                    chart_manifest=chart_manifest,
                    source=source,
                    station_key=station_key,
                    station_name=station_name,
                    chart_targets=chart_targets,
                    year_digests=year_digests,
                    output_dir=config.parquet_dir,
                    out_dir_path=out_dir_path,
                ):
                    manifest_dirty = True

        del timeseries_df, annual_max_df

    if excel_parallel_mode and excel_jobs:
        fallback_jobs: list[tuple[_ExcelGenerateJob, str, str, dict[int, str]]] = []
        future_to_job: dict[object, tuple[_ExcelGenerateJob, str, str, dict[int, str]]] = {}
        completed_excel_job_keys: set[tuple[str, str]] = set()
        pool_broken = False

        def _excel_job_key(job_tuple: tuple[_ExcelGenerateJob, str, str, dict[int, str]]) -> tuple[str, str]:
            job = job_tuple[0]
            return (job.source, job.station_key)

        try:
            with ProcessPoolExecutor(max_workers=excel_parallel_workers) as executor:
                for job_tuple in excel_jobs:
                    future = executor.submit(_run_excel_generate_job, job_tuple[0])
                    future_to_job[future] = job_tuple

                for future in as_completed(future_to_job):
                    job_tuple = future_to_job[future]
                    job, station_id, station_digest, year_digests = job_tuple
                    try:
                        job_result = future.result()
                    except BrokenProcessPool as exc:
                        pool_broken = True
                        logger(
                            "[generate][WARN] Excel 並列ワーカーが異常終了しました。"
                            f" 観測所={job.station_key} ({type(exc).__name__}: {exc})"
                            " 残りジョブを直列フォールバックします。"
                        )
                        fallback_jobs.append(job_tuple)
                        for pending_future, pending_job in future_to_job.items():
                            if pending_future is future:
                                continue
                            if not pending_future.done():
                                fallback_jobs.append(pending_job)
                        break
                    except Exception as exc:  # noqa: BLE001
                        logger(
                            "[generate][WARN] Excel 並列ジョブ失敗。"
                            f" 観測所={job.station_key} ({type(exc).__name__}: {exc})"
                            " 直列で再実行します。"
                        )
                        fallback_jobs.append(job_tuple)
                        continue

                    if job_result.output_path is not None:
                        excel_paths.append(job_result.output_path)
                        generated_excel_count += 1
                        excel_manifest[station_id] = {
                            "station_digest": station_digest,
                            "output_relpath": _to_relpath(Path(job_result.output_path), out_dir_path),
                            "year_digests": {str(year): digest for year, digest in sorted(year_digests.items())},
                        }
                        manifest_dirty = True
                    completed_excel_job_keys.add(_excel_job_key(job_tuple))
        except BrokenProcessPool as exc:
            pool_broken = True
            logger(
                "[generate][WARN] Excel 並列プールが利用不能になりました。"
                f" ({type(exc).__name__}: {exc}) 直列フォールバックへ切り替えます。"
            )

        if pool_broken:
            fallback_keys = {_excel_job_key(job_tuple) for job_tuple in fallback_jobs}
            for job_tuple in excel_jobs:
                job_key = _excel_job_key(job_tuple)
                if job_key in completed_excel_job_keys or job_key in fallback_keys:
                    continue
                fallback_jobs.append(job_tuple)
                fallback_keys.add(job_key)

        unique_fallback_jobs: list[tuple[_ExcelGenerateJob, str, str, dict[int, str]]] = []
        seen_fallback_keys: set[tuple[str, str]] = set()
        for job_tuple in fallback_jobs:
            job_key = _excel_job_key(job_tuple)
            if job_key in seen_fallback_keys:
                continue
            seen_fallback_keys.add(job_key)
            unique_fallback_jobs.append(job_tuple)

        for job_tuple in unique_fallback_jobs:
            job, station_id, station_digest, year_digests = job_tuple
            try:
                job_result: _ExcelGenerateJobResult = _run_excel_generate_job(job)
            except Exception as exc:  # noqa: BLE001
                logger(
                    "[generate][ERROR] Excel 直列フォールバック失敗。"
                    f" 観測所={job.station_key} ({type(exc).__name__}: {exc})"
                )
                all_errors.append(f"excel:{job.station_key}:{type(exc).__name__}: {exc}")
                continue

            if job_result.output_path is not None:
                excel_paths.append(job_result.output_path)
                generated_excel_count += 1
                excel_manifest[station_id] = {
                    "station_digest": station_digest,
                    "output_relpath": _to_relpath(Path(job_result.output_path), out_dir_path),
                    "year_digests": {str(year): digest for year, digest in sorted(year_digests.items())},
                }
                manifest_dirty = True

    if chart_parallel_mode and chart_jobs:
        fallback_jobs: list[_ChartGenerateJob] = []
        future_to_job: dict[object, _ChartGenerateJob] = {}
        completed_chart_job_keys: set[tuple[str, str, tuple[tuple[int, str], ...]]] = set()
        pool_broken = False

        def _chart_job_key(job: _ChartGenerateJob) -> tuple[str, str, tuple[tuple[int, str], ...]]:
            targets = tuple(job.chart_targets or [])
            return (job.source, job.station_key, targets)

        try:
            with ProcessPoolExecutor(max_workers=chart_parallel_workers) as executor:
                for job in chart_jobs:
                    future = executor.submit(_run_chart_generate_job, job)
                    future_to_job[future] = job

                for future in as_completed(future_to_job):
                    job = future_to_job[future]
                    try:
                        job_result = future.result()
                    except BrokenProcessPool as exc:
                        pool_broken = True
                        logger(
                            "[generate][WARN] グラフ並列ワーカーが異常終了しました。"
                            f" 観測所={job.station_key} ({type(exc).__name__}: {exc})"
                            " 残りジョブを直列フォールバックします。"
                        )
                        fallback_jobs.append(job)
                        for pending_future, pending_job in future_to_job.items():
                            if pending_future is future:
                                continue
                            if not pending_future.done():
                                fallback_jobs.append(pending_job)
                        break
                    except Exception as exc:  # noqa: BLE001
                        logger(
                            "[generate][WARN] グラフ並列ジョブ失敗。"
                            f" 観測所={job.station_key} ({type(exc).__name__}: {exc})"
                            " 直列で再実行します。"
                        )
                        fallback_jobs.append(job)
                        continue

                    chart_paths.extend(job_result.generated_paths)
                    generated_chart_count += len(job_result.generated_paths)
                    skipped_chart_count += int(job_result.skipped_count)
                    completed_chart_job_keys.add(_chart_job_key(job))
                    if _update_chart_manifest_entries(
                        chart_manifest=chart_manifest,
                        source=job.source,
                        station_key=job.station_key,
                        station_name=job_result.station_name or job.station_name,
                        chart_targets=job_result.chart_targets,
                        year_digests=job.year_digests,
                        output_dir=config.parquet_dir,
                        out_dir_path=out_dir_path,
                    ):
                        manifest_dirty = True
        except BrokenProcessPool as exc:
            pool_broken = True
            logger(
                "[generate][WARN] グラフ並列プールが利用不能になりました。"
                f" ({type(exc).__name__}: {exc}) 直列フォールバックへ切り替えます。"
            )

        if pool_broken:
            fallback_keys = {_chart_job_key(job) for job in fallback_jobs}
            for job in chart_jobs:
                job_key = _chart_job_key(job)
                if job_key in completed_chart_job_keys or job_key in fallback_keys:
                    continue
                fallback_jobs.append(job)
                fallback_keys.add(job_key)

        unique_fallback_jobs: list[_ChartGenerateJob] = []
        seen_fallback_keys: set[tuple[str, str, tuple[tuple[int, str], ...]]] = set()
        for job in fallback_jobs:
            job_key = _chart_job_key(job)
            if job_key in seen_fallback_keys:
                continue
            seen_fallback_keys.add(job_key)
            unique_fallback_jobs.append(job)

        for job in unique_fallback_jobs:
            try:
                job_result = _run_chart_generate_job(job)
            except Exception as exc:  # noqa: BLE001
                logger(
                    "[generate][ERROR] グラフ直列フォールバック失敗。"
                    f" 観測所={job.station_key} ({type(exc).__name__}: {exc})"
                )
                all_errors.append(f"chart:{job.station_key}:{type(exc).__name__}: {exc}")
                continue

            chart_paths.extend(job_result.generated_paths)
            generated_chart_count += len(job_result.generated_paths)
            skipped_chart_count += int(job_result.skipped_count)
            if _update_chart_manifest_entries(
                chart_manifest=chart_manifest,
                source=job.source,
                station_key=job.station_key,
                station_name=job_result.station_name or job.station_name,
                chart_targets=job_result.chart_targets,
                year_digests=job.year_digests,
                output_dir=config.parquet_dir,
                out_dir_path=out_dir_path,
            ):
                manifest_dirty = True

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
