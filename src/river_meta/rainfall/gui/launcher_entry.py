from __future__ import annotations

from typing import Callable

import tkinter as tk


def open_rainfall_app(
    *,
    parent: tk.Misc,
    on_open_other: Callable[[str], None] | None = None,
    on_close: Callable[[], None] | None = None,
    on_return_home: Callable[[], None] | None = None,
):
    from ..entry import configure_runtime
    from .app import show_rainfall

    configure_runtime()
    return show_rainfall(
        parent=parent,
        on_open_other=on_open_other,
        on_close=on_close,
        on_return_home=on_return_home,
    )
