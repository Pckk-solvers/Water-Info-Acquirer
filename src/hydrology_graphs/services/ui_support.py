from __future__ import annotations

"""互換レイヤー: 旧 services.ui_support は ui.view_models に移動済み。"""

from hydrology_graphs.ui.view_models import (  # noqa: F401
    PreviewChoices,
    build_batch_targets,
    build_preview_choices,
    graph_targets_from_precheck_items,
    parse_base_dates_text,
    selected_event_windows,
    selected_station_pairs,
)

__all__ = [
    "PreviewChoices",
    "parse_base_dates_text",
    "selected_event_windows",
    "selected_station_pairs",
    "graph_targets_from_precheck_items",
    "build_preview_choices",
    "build_batch_targets",
]
