from typing import Any

try:
    from .amedas import AmedasRunInput, AmedasRunResult, run_amedas_extract
except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency fallback
    AmedasRunInput = Any  # type: ignore[assignment]
    AmedasRunResult = Any  # type: ignore[assignment]
    _amedas_import_error = exc

    def run_amedas_extract(*args, **kwargs):  # type: ignore[no-redef]
        raise ModuleNotFoundError("run_amedas_extract requires optional dependency 'pdfplumber'") from _amedas_import_error

try:
    from .gpkg import GpkgRunInput, GpkgRunResult, run_csv_to_gpkg
except ModuleNotFoundError as exc:  # pragma: no cover - optional dependency fallback
    GpkgRunInput = Any  # type: ignore[assignment]
    GpkgRunResult = Any  # type: ignore[assignment]
    _gpkg_import_error = exc

    def run_csv_to_gpkg(*args, **kwargs):  # type: ignore[no-redef]
        raise ModuleNotFoundError("run_csv_to_gpkg requires optional GIS dependencies") from _gpkg_import_error

from .rainfall import RainfallAnalyzeResult, RainfallRunInput, run_rainfall_analyze, run_rainfall_collect
from .river_meta import RiverMetaRunInput, RiverMetaRunResult, collect_station_ids, run_river_meta
from .station_ids import StationIdsRunInput, StationIdsRunResult, run_station_ids_collect

__all__ = [
    "AmedasRunInput",
    "AmedasRunResult",
    "collect_station_ids",
    "GpkgRunInput",
    "GpkgRunResult",
    "RiverMetaRunInput",
    "RiverMetaRunResult",
    "run_amedas_extract",
    "run_csv_to_gpkg",
    "run_rainfall_analyze",
    "run_rainfall_collect",
    "run_river_meta",
    "run_station_ids_collect",
    "StationIdsRunInput",
    "StationIdsRunResult",
    "RainfallAnalyzeResult",
    "RainfallRunInput",
]
