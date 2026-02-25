from __future__ import annotations

import csv
from pathlib import Path

from .models import StationReport
from .normalizer import CSV_COLUMNS, extract_csv_meta_fields


def _years_to_text(years: list[int]) -> str:
    return ";".join(str(year) for year in sorted(set(years)))


def render_station_csv_row(report: StationReport) -> dict[str, str]:
    meta = extract_csv_meta_fields(report.site_meta)
    daily_years = sorted(set(report.available_years_daily))
    hourly_years = sorted(set(report.available_years_hourly))

    row = {
        "station_id": report.station_id,
        "station_name": meta["station_name"],
        "suikei_mei": meta["suikei_mei"],
        "kasen_mei": meta["kasen_mei"],
        "location": meta["location"],
        "latitude": meta["latitude"],
        "longitude": meta["longitude"],
        "kanrisha": meta["kanrisha"],
        "daily_years_count": str(len(daily_years)),
        "hourly_years_count": str(len(hourly_years)),
        "daily_years": _years_to_text(daily_years),
        "hourly_years": _years_to_text(hourly_years),
    }

    # Ensure stable keys and empty-string fallback for missing fields.
    return {column: row.get(column, "") for column in CSV_COLUMNS}


def write_station_csv(
    rows: list[dict[str, str]],
    path: str,
    *,
    encoding: str = "utf-8-sig",
    delimiter: str = ",",
) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding=encoding, newline="") as file:
        writer = csv.DictWriter(file, fieldnames=CSV_COLUMNS, delimiter=delimiter)
        writer.writeheader()
        writer.writerows(rows)
