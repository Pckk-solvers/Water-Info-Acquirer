from .constants import ANNUAL_GRAPH_TYPES, EVENT_GRAPH_TYPES, GRAPH_REQUIREMENTS, GRAPH_TYPES
from .logic import (
    annual_max_by_year,
    annual_max_series,
    event_window_bounds,
    ensure_graph_type_supported,
    extract_event_series,
    has_min_years,
    is_event_graph,
    required_metric_interval,
    threshold_key,
    validate_event_series_complete,
)
from .models import GraphTarget, ThresholdLine, ThresholdRecord

__all__ = [
    "ANNUAL_GRAPH_TYPES",
    "EVENT_GRAPH_TYPES",
    "GRAPH_REQUIREMENTS",
    "GRAPH_TYPES",
    "GraphTarget",
    "ThresholdLine",
    "ThresholdRecord",
    "annual_max_by_year",
    "annual_max_series",
    "event_window_bounds",
    "ensure_graph_type_supported",
    "extract_event_series",
    "has_min_years",
    "is_event_graph",
    "required_metric_interval",
    "threshold_key",
    "validate_event_series_complete",
]
"""ドメイン層。

グラフ種別、入力条件、窓切り出し、年最大算出などの
副作用を持たないロジックをまとめる。
"""
