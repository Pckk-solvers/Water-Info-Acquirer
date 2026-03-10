from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from river_meta.rainfall.outputs.analysis import build_annual_max_dataframe, build_hourly_timeseries_dataframe
from river_meta.rainfall.outputs.chart_exporter import export_rainfall_charts
from river_meta.rainfall.outputs.excel_exporter import export_station_rainfall_excel
from river_meta.rainfall.storage.manifest import build_chart_id
from river_meta.rainfall.storage.parquet_store import (
    ParquetEntry,
    build_parquet_path,
    load_and_concat_monthly_parquets,
    load_records_parquet,
)

from .common import build_chart_output_path, build_excel_output_path, to_relpath

if TYPE_CHECKING:
    import pandas as pd


@dataclass(slots=True)
class ChartGenerateJob:
    source: str
    station_key: str
    station_name: str
    parquet_dir: str
    station_entries: list[ParquetEntry]
    year_digests: dict[int, str]
    chart_targets: list[tuple[int, str]] | None = None
    diff_mode: bool = False
    previous_chart_records: dict[tuple[int, str], tuple[str, str]] | None = None


@dataclass(slots=True)
class ChartGenerateJobResult:
    generated_paths: list[str]
    chart_targets: list[tuple[int, str]]
    skipped_count: int = 0
    station_name: str = ""


@dataclass(slots=True)
class ExcelGenerateJob:
    source: str
    station_key: str
    station_name: str
    parquet_dir: str
    station_entries: list[ParquetEntry]
    decimal_places: int


@dataclass(slots=True)
class ExcelGenerateJobResult:
    output_path: str | None
    station_name: str = ""


def collect_parquet_paths_for_entry(output_dir: str | Path, entry: ParquetEntry) -> list[Path]:
    if entry.source == "jma":
        return [
            build_parquet_path(output_dir, entry.source, entry.station_key, entry.year, month=month)
            for month in range(1, 13)
        ]
    return [build_parquet_path(output_dir, entry.source, entry.station_key, entry.year)]


def load_source_dataframe_for_station_entries(
    parquet_dir: str | Path,
    station_entries: list[ParquetEntry],
) -> tuple["pd.DataFrame | None", str]:
    import pandas as pd

    source_dfs: list[pd.DataFrame] = []
    station_name = ""

    for entry in station_entries:
        if entry.source == "jma":
            source_df = load_and_concat_monthly_parquets(
                parquet_dir, entry.source, entry.station_key, entry.year,
            )
        else:
            pq_path = build_parquet_path(
                parquet_dir, entry.source, entry.station_key, entry.year,
            )
            source_df = load_records_parquet(pq_path)

        if source_df is None or source_df.empty:
            continue

        if not station_name and "station_name" in source_df.columns:
            station_name = str(source_df["station_name"].iloc[0])
        source_dfs.append(source_df)

    if not source_dfs:
        return None, station_name

    combined = pd.concat(source_dfs, ignore_index=True) if len(source_dfs) > 1 else source_dfs[0]
    return combined, station_name


def build_chart_targets(chart_target_df: "pd.DataFrame") -> list[tuple[int, str]]:
    return [(int(row["年"]), str(row["指標"])) for _, row in chart_target_df.iterrows()]


def build_chart_target_df(
    annual_max_df: "pd.DataFrame",
    chart_targets: list[tuple[int, str]],
) -> "pd.DataFrame":
    import pandas as pd

    if annual_max_df is None or annual_max_df.empty or not chart_targets:
        return annual_max_df.iloc[0:0].copy() if annual_max_df is not None else pd.DataFrame()

    target_set = {(int(year), str(metric)) for year, metric in chart_targets}
    mask = annual_max_df.apply(
        lambda row: (int(row["年"]), str(row["指標"])) in target_set,
        axis=1,
    )
    return annual_max_df.loc[mask].copy()


def run_chart_generate_job(job: ChartGenerateJob) -> ChartGenerateJobResult:
    source_df, inferred_station_name = load_source_dataframe_for_station_entries(
        job.parquet_dir,
        job.station_entries,
    )
    if source_df is None or source_df.empty:
        return ChartGenerateJobResult(generated_paths=[], chart_targets=[], skipped_count=0, station_name="")

    station_name = job.station_name or inferred_station_name
    timeseries_df = build_hourly_timeseries_dataframe(source_df)
    annual_max_df = build_annual_max_dataframe(timeseries_df)
    chart_target_df = annual_max_df
    skipped_count = 0
    if job.diff_mode:
        previous_chart_records = job.previous_chart_records or {}
        target_indices: list[int] = []
        out_dir_path = Path(job.parquet_dir)
        for idx, row in annual_max_df.iterrows():
            year = int(row["年"])
            metric = str(row["指標"])
            previous_record = previous_chart_records.get((year, metric))
            if previous_record is None:
                target_indices.append(idx)
                continue
            prev_year_digest, prev_chart_relpath = previous_record
            if (
                prev_year_digest
                and prev_year_digest == job.year_digests.get(year, "")
                and prev_chart_relpath
                and (out_dir_path / prev_chart_relpath).exists()
            ):
                skipped_count += 1
                continue
            target_indices.append(idx)
        chart_target_df = annual_max_df.loc[target_indices].copy() if target_indices else annual_max_df.iloc[0:0]
    elif job.chart_targets is not None:
        chart_target_df = build_chart_target_df(annual_max_df, job.chart_targets)

    chart_targets = build_chart_targets(chart_target_df)
    if timeseries_df.empty or annual_max_df.empty or chart_target_df.empty:
        return ChartGenerateJobResult(
            generated_paths=[],
            chart_targets=[],
            skipped_count=skipped_count,
            station_name=station_name,
        )

    generated = export_rainfall_charts(
        timeseries_df,
        chart_target_df,
        output_dir=str(Path(job.parquet_dir) / "charts"),
        station_key=job.station_key,
        station_name=station_name,
        should_stop=None,
    )
    return ChartGenerateJobResult(
        generated_paths=[str(path) for path in generated],
        chart_targets=chart_targets,
        skipped_count=skipped_count,
        station_name=station_name,
    )


def run_excel_generate_job(job: ExcelGenerateJob) -> ExcelGenerateJobResult:
    source_df, inferred_station_name = load_source_dataframe_for_station_entries(
        job.parquet_dir,
        job.station_entries,
    )
    if source_df is None or source_df.empty:
        return ExcelGenerateJobResult(output_path=None, station_name="")

    station_name = job.station_name or inferred_station_name
    timeseries_df = build_hourly_timeseries_dataframe(source_df)
    annual_max_df = build_annual_max_dataframe(timeseries_df)
    if timeseries_df.empty:
        return ExcelGenerateJobResult(output_path=None, station_name=station_name)

    output_path = build_excel_output_path(job.parquet_dir, job.station_key, station_name)
    exported = export_station_rainfall_excel(
        timeseries_df,
        annual_max_df,
        output_path=str(output_path),
        decimal_places=job.decimal_places,
    )
    return ExcelGenerateJobResult(
        output_path=(str(exported) if exported is not None else None),
        station_name=station_name,
    )


def update_chart_manifest_entries(
    *,
    chart_manifest: dict[str, object],
    source: str,
    station_key: str,
    station_name: str,
    chart_targets: list[tuple[int, str]],
    year_digests: dict[int, str],
    output_dir: str | Path,
    out_dir_path: Path,
) -> bool:
    updated = False
    for year, metric in chart_targets:
        expected_output = build_chart_output_path(
            output_dir,
            station_key,
            station_name,
            int(year),
            str(metric),
        )
        if not expected_output.exists():
            continue
        chart_id = build_chart_id(source, station_key, int(year), str(metric))
        chart_manifest[chart_id] = {
            "year_digest": year_digests.get(int(year), ""),
            "output_relpath": to_relpath(expected_output, out_dir_path),
        }
        updated = True
    return updated
