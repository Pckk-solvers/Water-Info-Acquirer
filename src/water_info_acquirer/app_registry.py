from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass


OpenAppFn = Callable[..., object]


@dataclass(frozen=True, slots=True)
class AppDefinition:
    key: str
    title_key: str
    open_app: OpenAppFn
    description: str = ""
    help_url: str = ""
    enabled: bool = True


APP_DEFINITIONS: tuple[AppDefinition, ...] = (
    AppDefinition(
        key="water",
        title_key="water_info",
        open_app=lambda *, parent, on_open_other, on_close, on_return_home=None, developer_mode=False: __import__(
            "water_info.launcher_entry", fromlist=["open_water_app"]
        ).open_water_app(
            parent=parent,
            on_open_other=on_open_other,
            on_close=on_close,
            on_return_home=on_return_home,
        ),
        description="国土交通省の水文データ取得GUIを起動します。",
        help_url="https://pckk-solvers.github.io/Water-Info-Acquirer/user/water-info/",
    ),
    AppDefinition(
        key="jma",
        title_key="jma",
        open_app=lambda *, parent, on_open_other, on_close, on_return_home=None, developer_mode=False: __import__(
            "jma_rainfall_pipeline.launcher_entry", fromlist=["open_jma_app"]
        ).open_jma_app(
            parent=parent,
            on_open_other=on_open_other,
            on_close=on_close,
            on_return_home=on_return_home,
        ),
        description="気象庁の雨量データ取得GUIを起動します。",
        help_url="https://pckk-solvers.github.io/Water-Info-Acquirer/user/jma-rainfall/",
    ),
    AppDefinition(
        key="rainfall",
        title_key="rainfall",
        open_app=lambda *, parent, on_open_other, on_close, on_return_home=None, developer_mode=False: __import__(
            "river_meta.rainfall.gui.launcher_entry", fromlist=["open_rainfall_app"]
        ).open_rainfall_app(
            parent=parent,
            on_open_other=on_open_other,
            on_close=on_close,
            on_return_home=on_return_home,
        ),
        description="雨量の収集済みParquet整理・期間抽出GUIを起動します。",
        help_url="https://pckk-solvers.github.io/Water-Info-Acquirer/user/rainfall/",
    ),
    AppDefinition(
        key="hydrology_graphs",
        title_key="hydrology_graphs",
        open_app=lambda *, parent, on_open_other, on_close, on_return_home=None, developer_mode=False: __import__(
            "hydrology_graphs.launcher_entry", fromlist=["open_hydrology_graphs_app"]
        ).open_hydrology_graphs_app(
            parent=parent,
            on_open_other=on_open_other,
            on_close=on_close,
            on_return_home=on_return_home,
            developer_mode=developer_mode,
        ),
        description="Parquet から6種の水文グラフを一括生成するGUIを起動します。",
        help_url="https://pckk-solvers.github.io/Water-Info-Acquirer/dev/hydrology-graphs-platform/",
    ),
)


APP_DEFINITION_BY_KEY = {definition.key: definition for definition in APP_DEFINITIONS}
