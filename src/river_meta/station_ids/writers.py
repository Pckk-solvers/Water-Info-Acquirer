from __future__ import annotations

import csv
from pathlib import Path


def write_ids_txt(path: str, station_ids: list[str]) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8", newline="\n") as file:
        for station_id in station_ids:
            file.write(station_id + "\n")


def write_pref_csv(path: str, rows: list[dict[str, str]]) -> None:
    output_path = Path(path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8-sig", newline="") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=["ken_code", "prefecture", "station_id"],
        )
        writer.writeheader()
        writer.writerows(rows)
