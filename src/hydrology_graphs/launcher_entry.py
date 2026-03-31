from __future__ import annotations

from typing import Callable

import tkinter as tk

from .ui.app import show_hydrology_graphs

"""ランチャーから Hydrology Graphs を開くための入口。"""


def open_hydrology_graphs_app(
    *,
    parent: tk.Misc,
    on_open_other: Callable[[str], None] | None = None,
    on_close: Callable[[], None] | None = None,
    on_return_home: Callable[[], None] | None = None,
    developer_mode: bool = False,
):
    """アプリ本体の UI を起動する。"""

    return show_hydrology_graphs(
        parent=parent,
        on_open_other=on_open_other,
        on_close=on_close,
        on_return_home=on_return_home,
        developer_mode=developer_mode,
    )
