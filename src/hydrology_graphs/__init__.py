from .domain.constants import ANNUAL_GRAPH_TYPES, EVENT_GRAPH_TYPES, GRAPH_TYPES
from .domain.logic import (
    annual_max_by_year,
    annual_max_series,
    event_capture_window_bounds,
    event_window_bounds,
    ensure_graph_type_supported,
    extract_event_series,
    has_min_years,
    is_event_graph,
    required_metric_interval,
    threshold_key,
    validate_event_series_complete,
)
from .domain.models import GraphTarget, ThresholdLine, ThresholdRecord
from .services.dto import (
    BatchRunInput,
    BatchRunItemResult,
    BatchRunResult,
    BatchSummary,
    BatchTarget,
    PrecheckInput,
    PrecheckItem,
    PrecheckResult,
    PrecheckSummary,
    PreviewInput,
    PreviewResult,
)
from .services.usecases import HydrologyGraphService

__all__ = [
    "ANNUAL_GRAPH_TYPES",
    "EVENT_GRAPH_TYPES",
    "GRAPH_TYPES",
    "GraphTarget",
    "ThresholdLine",
    "ThresholdRecord",
    "BatchRunInput",
    "BatchRunItemResult",
    "BatchRunResult",
    "BatchSummary",
    "BatchTarget",
    "PrecheckInput",
    "PrecheckItem",
    "PrecheckResult",
    "PrecheckSummary",
    "PreviewInput",
    "PreviewResult",
    "HydrologyGraphService",
    "annual_max_by_year",
    "annual_max_series",
    "event_capture_window_bounds",
    "event_window_bounds",
    "ensure_graph_type_supported",
    "extract_event_series",
    "has_min_years",
    "is_event_graph",
    "required_metric_interval",
    "threshold_key",
    "validate_event_series_complete",
]
"""水文グラフ生成機能のルートパッケージ。

この配下には、Parquet から水文・降雨グラフを生成するための
domain / io / services / render / ui の各層が入る。
"""
