from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING

from river_meta.rainfall.storage.parquet_store import ParquetEntry
from .models import RainfallDataset
from river_meta.rainfall.support.period import normalize_collection_order as _normalize_collection_order

if TYPE_CHECKING:
    import pandas as pd


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
    collection_order: str = "station_year"
    include_raw: bool = False

    def __post_init__(self) -> None:
        self.collection_order = _normalize_collection_order(self.collection_order)


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
    excel_parallel_enabled: bool = False
    excel_parallel_workers: int = 1
    chart_parallel_enabled: bool = False
    chart_parallel_workers: int = 1
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
