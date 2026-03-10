from __future__ import annotations

from typing import Callable

import tkinter as tk

from .gui.app import show_jma


def open_jma_app(
    *,
    parent: tk.Misc,
    on_open_other: Callable[[str], None] | None = None,
    on_close: Callable[[], None] | None = None,
    on_return_home: Callable[[], None] | None = None,
):
    return show_jma(
        parent=parent,
        on_open_other=on_open_other,
        on_close=on_close,
        on_return_home=on_return_home,
    )
