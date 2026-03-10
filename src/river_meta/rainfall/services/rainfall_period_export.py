from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Callable
import csv

import pandas as pd

from river_meta.rainfall.outputs.analysis import build_hourly_timeseries_dataframe
from river_meta.rainfall.storage.parquet_store import (
    build_parquet_path,
    load_and_concat_monthly_parquets,
    load_records_parquet,
)

from river_meta.rainfall.support.common import sanitize_path_token

if TYPE_CHECKING:
    from pandas import DataFrame


LogFn = Callable[[str], None]


@dataclass(slots=True)
class RainfallParquetPeriodExportInput:
    parquet_dir: str
    output_dir: str
    source: str
    station_key: str
    start_date: date | datetime | str
    end_date: date | datetime | str
    station_name: str = ""
    display_station_code: str = ""
    available_years: list[int] = field(default_factory=list)


@dataclass(slots=True)
class RainfallParquetPeriodExportResult:
    csv_path: str | None
    row_count: int
    source: str
    station_key: str
    station_name: str
    start_date: str
    end_date: str
    errors: list[str] = field(default_factory=list)


@dataclass(slots=True)
class RainfallParquetPeriodExportTarget:
    source: str
    station_key: str
    start_date: date | datetime | str
    end_date: date | datetime | str
    parquet_dir: str = ""
    station_name: str = ""
    display_station_code: str = ""
    available_years: list[int] = field(default_factory=list)


@dataclass(slots=True)
class RainfallParquetPeriodBatchExportInput:
    parquet_dir: str
    output_dir: str
    targets: list[RainfallParquetPeriodExportTarget] = field(default_factory=list)


@dataclass(slots=True)
class RainfallParquetPeriodBatchExportResult:
    results: list[RainfallParquetPeriodExportResult] = field(default_factory=list)
    csv_paths: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


SETTINGS_CSV_COLUMNS = [
    "parquet_dir",
    "source",
    "station_key",
    "station_code",
    "station_name",
    "start_date",
    "end_date",
]


def run_rainfall_parquet_period_export(
    config: RainfallParquetPeriodExportInput,
    *,
    log: LogFn | None = None,
) -> RainfallParquetPeriodExportResult:
    logger = log or (lambda _msg: None)
    parquet_root = _resolve_parquet_output_root(config.parquet_dir)
    start_day = _normalize_date(config.start_date, "開始日")
    end_day = _normalize_date(config.end_date, "終了日")
    source_df = _load_station_range_dataframe(
        parquet_dir=str(parquet_root),
        source=config.source,
        station_key=config.station_key,
        start_year=start_day.year,
        end_year=end_day.year,
    )
    if source_df is None or source_df.empty:
        return RainfallParquetPeriodExportResult(
            csv_path=None,
            row_count=0,
            source=config.source,
            station_key=config.station_key,
            station_name=config.station_name,
            start_date=start_day.isoformat(),
            end_date=end_day.isoformat(),
            errors=[f"No parquet data found for station/range: {config.station_key}"],
        )

    station_name = str(config.station_name or _infer_station_name(source_df)).strip()
    timeseries_df = build_hourly_timeseries_dataframe(source_df)
    if start_day > end_day:
        return RainfallParquetPeriodExportResult(
            csv_path=None,
            row_count=0,
            source=config.source,
            station_key=config.station_key,
            station_name=station_name,
            start_date=start_day.isoformat(),
            end_date=end_day.isoformat(),
            errors=["開始日は終了日以前で指定してください。"],
        )

    parquet_root = _resolve_parquet_output_root(config.parquet_dir)
    if not (parquet_root / "parquet").exists():
        return RainfallParquetPeriodExportResult(
            csv_path=None,
            row_count=0,
            source=config.source,
            station_key=config.station_key,
            station_name=config.station_name,
            start_date=start_day.isoformat(),
            end_date=end_day.isoformat(),
            errors=["parquet/ directory not found"],
        )
    range_df = _build_range_csv_dataframe(timeseries_df, start_day, end_day)

    output_dir = Path(config.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    display_station_code = str(config.display_station_code or _resolve_display_station_code(config.source, config.station_key)).strip()
    safe_station_name = sanitize_path_token(station_name) or sanitize_path_token(config.station_key)
    safe_period = start_day.isoformat() if start_day == end_day else f"{start_day.isoformat()}_{end_day.isoformat()}"
    filename = (
        f"{sanitize_path_token(config.source)}_"
        f"{sanitize_path_token(display_station_code)}_"
        f"{safe_station_name}_"
        f"{safe_period}.csv"
    )
    csv_path = output_dir / filename
    range_df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    logger(f"[period-csv] 出力完了: {csv_path}")

    return RainfallParquetPeriodExportResult(
        csv_path=str(csv_path),
        row_count=len(range_df),
        source=config.source,
        station_key=config.station_key,
        station_name=station_name,
        start_date=start_day.isoformat(),
        end_date=end_day.isoformat(),
        errors=[],
    )


def run_rainfall_parquet_period_batch_export(
    config: RainfallParquetPeriodBatchExportInput,
    *,
    log: LogFn | None = None,
) -> RainfallParquetPeriodBatchExportResult:
    logger = log or (lambda _msg: None)
    results: list[RainfallParquetPeriodExportResult] = []
    csv_paths: list[str] = []
    all_errors: list[str] = []

    for target in config.targets:
        result = run_rainfall_parquet_period_export(
            RainfallParquetPeriodExportInput(
                parquet_dir=target.parquet_dir or config.parquet_dir,
                output_dir=config.output_dir,
                source=target.source,
                station_key=target.station_key,
                station_name=target.station_name,
                display_station_code=target.display_station_code,
                start_date=target.start_date,
                end_date=target.end_date,
                available_years=list(target.available_years),
            ),
            log=log,
        )
        results.append(result)
        if result.csv_path:
            csv_paths.append(result.csv_path)
        for error in result.errors:
            all_errors.append(f"{target.source}:{target.station_key}: {error}")
    logger(f"[period-csv] 複数観測所出力: {len(csv_paths)}件")
    return RainfallParquetPeriodBatchExportResult(
        results=results,
        csv_paths=sorted(csv_paths),
        errors=all_errors,
    )


def export_period_targets_csv(path: str | Path, targets: list[RainfallParquetPeriodExportTarget]) -> Path:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SETTINGS_CSV_COLUMNS)
        writer.writeheader()
        for target in targets:
            writer.writerow(
                {
                    "parquet_dir": target.parquet_dir,
                    "source": target.source,
                    "station_key": target.station_key,
                    "station_code": target.display_station_code,
                    "station_name": target.station_name,
                    "start_date": str(target.start_date or "").strip(),
                    "end_date": str(target.end_date or "").strip(),
                }
            )
    return output_path


def load_period_targets_csv(path: str | Path) -> list[RainfallParquetPeriodExportTarget]:
    csv_path = Path(path)
    if not csv_path.exists():
        raise FileNotFoundError(str(csv_path))
    targets: list[RainfallParquetPeriodExportTarget] = []
    with csv_path.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            source = str(row.get("source") or "").strip()
            station_key = str(row.get("station_key") or "").strip()
            if not source or not station_key:
                continue
            targets.append(
                RainfallParquetPeriodExportTarget(
                    parquet_dir=str(row.get("parquet_dir") or "").strip(),
                    source=source,
                    station_key=station_key,
                    display_station_code=str(row.get("station_code") or "").strip(),
                    station_name=str(row.get("station_name") or "").strip(),
                    start_date=str(row.get("start_date") or "").strip() or None,
                    end_date=str(row.get("end_date") or "").strip() or None,
                )
            )
    return targets


def _normalize_date(value: date | datetime | str, label: str) -> date:
    if isinstance(value, date) and not isinstance(value, datetime):
        return value
    if isinstance(value, datetime):
        return value.date()
    try:
        parsed = pd.to_datetime(value, errors="raise")
    except Exception as exc:  # noqa: BLE001
        raise ValueError(f"{label}は YYYY-MM-DD 形式で指定してください。") from exc
    return parsed.date()


def _resolve_parquet_output_root(raw_path: str | Path) -> Path:
    path = Path(raw_path)
    if path.name.lower() == "parquet":
        return path.parent
    return path


def _load_station_range_dataframe(
    *,
    parquet_dir: str,
    source: str,
    station_key: str,
    start_year: int,
    end_year: int,
) -> "DataFrame | None":
    years = list(range(start_year, end_year + 1))
    frames: list[DataFrame] = []
    for year in years:
        if source == "jma":
            frame = load_and_concat_monthly_parquets(parquet_dir, source, station_key, year)
        else:
            frame = load_records_parquet(build_parquet_path(parquet_dir, source, station_key, year))
        if frame is None or frame.empty:
            continue
        frames.append(frame)
    if not frames:
        return None
    return pd.concat(frames, ignore_index=True)


def _infer_station_name(source_df: "DataFrame") -> str:
    if "station_name" not in source_df.columns or source_df.empty:
        return ""
    return str(source_df["station_name"].iloc[0]).strip()


def _resolve_display_station_code(source: str, station_key: str) -> str:
    raw = str(station_key)
    if source == "jma" and "_" in raw:
        return raw.split("_", 1)[1]
    return raw




def _build_range_csv_dataframe(timeseries_df: "DataFrame", start_day: date, end_day: date) -> "DataFrame":
    start_at = datetime(start_day.year, start_day.month, start_day.day, 0, 0, 0)
    hours = ((end_day - start_day).days + 1) * 24
    full_index = pd.date_range(start_at, periods=hours, freq="h")
    if timeseries_df is None or timeseries_df.empty:
        rainfall_series = pd.Series([None] * hours, index=full_index, dtype="float64")
    else:
        frame = timeseries_df.copy()
        frame["観測時刻"] = pd.to_datetime(frame["観測時刻"], errors="coerce")
        frame = frame.dropna(subset=["観測時刻"])
        end_at = datetime(end_day.year, end_day.month, end_day.day, 23, 0, 0)
        frame = frame[(frame["観測時刻"] >= start_at) & (frame["観測時刻"] <= end_at)]
        rainfall_series = pd.to_numeric(
            frame.set_index("観測時刻")["1時間雨量(mm)"].reindex(full_index),
            errors="coerce",
        )

    return pd.DataFrame(
        {
            "date": [idx.strftime("%Y-%m-%d") for idx in full_index],
            "hour": [idx.hour + 1 for idx in full_index],
            "rainfall": rainfall_series.tolist(),
        }
    )
