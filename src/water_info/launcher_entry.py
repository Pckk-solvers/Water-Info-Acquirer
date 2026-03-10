from __future__ import annotations

from typing import Callable

import tkinter as tk

from .entry import show_water


def open_water_app(
    *,
    parent: tk.Misc,
    on_open_other: Callable[[str], None] | None = None,
    on_close: Callable[[], None] | None = None,
    on_return_home: Callable[[], None] | None = None,
):
    return show_water(
        parent=parent,
        on_open_other=on_open_other,
        on_close=on_close,
        on_return_home=on_return_home,
    )
