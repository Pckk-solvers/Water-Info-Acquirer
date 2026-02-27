from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from river_meta.gpkg.convert import csv_to_gpkg


LogFn = Callable[[str], None]


@dataclass(slots=True)
class GpkgRunInput:
    in_csv: str
    out_gpkg: str
    layer: str = "stations"
    lat_col: str = "latitude"
    lon_col: str = "longitude"
    encoding: str = "utf-8-sig"
    out_epsg: int = 4326


@dataclass(slots=True)
class GpkgRunResult:
    input_rows: int
    valid_rows: int
    invalid_rows: int
    output_epsg: int
    out_gpkg: str


def _noop_log(_: str) -> None:
    return None


def run_csv_to_gpkg(config: GpkgRunInput, *, log: LogFn | None = None) -> GpkgRunResult:
    logger = log or _noop_log
    stats = csv_to_gpkg(
        in_csv=config.in_csv,
        out_gpkg=config.out_gpkg,
        layer=config.layer,
        lat_col=config.lat_col,
        lon_col=config.lon_col,
        encoding=config.encoding,
        output_epsg=config.out_epsg,
    )
    logger(
        "[river-gpkg] done: "
        f"input={stats.input_rows}, valid={stats.valid_rows}, invalid={stats.invalid_rows}, epsg={stats.output_epsg}"
    )
    return GpkgRunResult(
        input_rows=stats.input_rows,
        valid_rows=stats.valid_rows,
        invalid_rows=stats.invalid_rows,
        output_epsg=stats.output_epsg,
        out_gpkg=config.out_gpkg,
    )
