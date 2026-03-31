from __future__ import annotations

from dataclasses import dataclass

"""グラフ種別と、それぞれに必要な計算条件を定義する定数群。"""

GRAPH_HYETOGRAPH = "hyetograph"
GRAPH_HYDRO_DISCHARGE = "hydrograph_discharge"
GRAPH_HYDRO_WATER_LEVEL = "hydrograph_water_level"
GRAPH_ANNUAL_RAINFALL = "annual_max_rainfall"
GRAPH_ANNUAL_DISCHARGE = "annual_max_discharge"
GRAPH_ANNUAL_WATER_LEVEL = "annual_max_water_level"

GRAPH_TYPES: tuple[str, ...] = (
    GRAPH_HYETOGRAPH,
    GRAPH_HYDRO_DISCHARGE,
    GRAPH_HYDRO_WATER_LEVEL,
    GRAPH_ANNUAL_RAINFALL,
    GRAPH_ANNUAL_DISCHARGE,
    GRAPH_ANNUAL_WATER_LEVEL,
)

EVENT_GRAPH_TYPES: tuple[str, ...] = (
    GRAPH_HYETOGRAPH,
    GRAPH_HYDRO_DISCHARGE,
    GRAPH_HYDRO_WATER_LEVEL,
)

ANNUAL_GRAPH_TYPES: tuple[str, ...] = (
    GRAPH_ANNUAL_RAINFALL,
    GRAPH_ANNUAL_DISCHARGE,
    GRAPH_ANNUAL_WATER_LEVEL,
)


@dataclass(frozen=True, slots=True)
class GraphRequirement:
    """グラフ種別ごとに必要なメトリクスと粒度を表す。"""

    metric: str
    interval: str
    event_graph: bool


GRAPH_REQUIREMENTS: dict[str, GraphRequirement] = {
    GRAPH_HYETOGRAPH: GraphRequirement(metric="rainfall", interval="1hour", event_graph=True),
    GRAPH_HYDRO_DISCHARGE: GraphRequirement(
        metric="discharge",
        interval="1hour",
        event_graph=True,
    ),
    GRAPH_HYDRO_WATER_LEVEL: GraphRequirement(
        metric="water_level",
        interval="1hour",
        event_graph=True,
    ),
    GRAPH_ANNUAL_RAINFALL: GraphRequirement(
        metric="rainfall",
        interval="1hour",
        event_graph=False,
    ),
    GRAPH_ANNUAL_DISCHARGE: GraphRequirement(
        metric="discharge",
        interval="1hour",
        event_graph=False,
    ),
    GRAPH_ANNUAL_WATER_LEVEL: GraphRequirement(
        metric="water_level",
        interval="1hour",
        event_graph=False,
    ),
}
