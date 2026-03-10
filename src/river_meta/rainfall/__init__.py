from .outputs.analysis import (
    build_annual_max_dataframe,
    build_hourly_timeseries_dataframe,
    build_station_summary_dataframe,
    year_to_japanese_era,
)
from .outputs.chart_exporter import export_rainfall_charts
from .outputs.excel_exporter import export_station_rainfall_excel
from .storage.parquet_store import ParquetEntry, find_missing_months, scan_parquet_dir
from .sources.jma.adapter import fetch_jma_rainfall
from .domain.models import (
    JMAStationInput,
    RainfallDataset,
    RainfallQuery,
    RainfallRecord,
    WaterInfoStationInput,
)
from .domain.normalizer import (
    normalize_interval_token,
    normalize_observed_at,
    normalize_rainfall_value,
    normalize_source_token,
)
from .sources.jma.station_index import (
    default_station_index_path,
    load_station_index,
    resolve_jma_station_codes_from_prefectures,
    resolve_jma_stations_from_codes,
    resolve_jma_stations_from_prefectures,
)
from .sources.water_info.adapter import fetch_waterinfo_rainfall
from .sources.water_info.station_index import resolve_waterinfo_station_codes_from_prefectures


def build_jma_station_index(*, output_path: str | None = None):
    from .commands.build_jma_station_index import build_jma_station_index as _build_jma_station_index

    return _build_jma_station_index(output_path=output_path)

__all__ = [
    "fetch_jma_rainfall",
    "fetch_waterinfo_rainfall",
    "build_jma_station_index",
    "build_hourly_timeseries_dataframe",
    "build_annual_max_dataframe",
    "build_station_summary_dataframe",
    "year_to_japanese_era",
    "export_rainfall_charts",
    "export_station_rainfall_excel",
    "ParquetEntry",
    "scan_parquet_dir",
    "find_missing_months",
    "JMAStationInput",
    "RainfallDataset",
    "RainfallQuery",
    "RainfallRecord",
    "WaterInfoStationInput",
    "normalize_interval_token",
    "normalize_observed_at",
    "normalize_rainfall_value",
    "normalize_source_token",
    "default_station_index_path",
    "load_station_index",
    "resolve_jma_station_codes_from_prefectures",
    "resolve_jma_stations_from_codes",
    "resolve_jma_stations_from_prefectures",
    "resolve_waterinfo_station_codes_from_prefectures",
]
