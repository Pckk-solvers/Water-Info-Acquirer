from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import geopandas as gpd
import pandas as pd
from shapely.geometry import Point

from .latlon import parse_latlon_pair


ALLOWED_OUTPUT_EPSG = {4326, *range(6669, 6689)}


@dataclass(slots=True)
class ConvertStats:
    input_rows: int
    valid_rows: int
    invalid_rows: int
    output_epsg: int


def validate_output_epsg(value: int) -> int:
    if value not in ALLOWED_OUTPUT_EPSG:
        raise ValueError(
            "unsupported output EPSG. Use EPSG:4326 or EPSG:6669-6688."
        )
    return value


def csv_to_gpkg(
    *,
    in_csv: str,
    out_gpkg: str,
    layer: str = "stations",
    lat_col: str = "latitude",
    lon_col: str = "longitude",
    encoding: str = "utf-8-sig",
    output_epsg: int = 4326,
) -> ConvertStats:
    output_epsg = validate_output_epsg(output_epsg)
    frame = pd.read_csv(in_csv, encoding=encoding)
    if lat_col not in frame.columns:
        raise ValueError(f"latitude column not found: {lat_col}")
    if lon_col not in frame.columns:
        raise ValueError(f"longitude column not found: {lon_col}")

    input_rows = len(frame.index)
    lat_values: list[float | None] = []
    lon_values: list[float | None] = []
    valid_mask: list[bool] = []

    for _, row in frame.iterrows():
        lat, lon = parse_latlon_pair(row.get(lat_col), row.get(lon_col))
        is_valid = lat is not None and lon is not None
        valid_mask.append(is_valid)
        lat_values.append(lat)
        lon_values.append(lon)

    frame = frame.copy()
    frame["__lat"] = lat_values
    frame["__lon"] = lon_values
    frame["parse_error"] = [not is_valid for is_valid in valid_mask]

    valid_frame = frame[frame["parse_error"] == False].copy()  # noqa: E712
    geometry = [Point(lon, lat) for lat, lon in zip(valid_frame["__lat"], valid_frame["__lon"], strict=True)]
    geo = gpd.GeoDataFrame(valid_frame.drop(columns=["__lat", "__lon"]), geometry=geometry, crs="EPSG:4326")
    if output_epsg != 4326:
        geo = geo.to_crs(epsg=output_epsg)

    output_path = Path(out_gpkg)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    geo.to_file(output_path, layer=layer, driver="GPKG")

    valid_rows = len(valid_frame.index)
    return ConvertStats(
        input_rows=input_rows,
        valid_rows=valid_rows,
        invalid_rows=input_rows - valid_rows,
        output_epsg=output_epsg,
    )
