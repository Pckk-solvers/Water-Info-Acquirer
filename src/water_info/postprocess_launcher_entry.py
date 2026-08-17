"""水文統計後処理GUIのランチャー入口。"""

from __future__ import annotations

from typing import Callable
import tkinter as tk

from .ui.postprocess_app import open_postprocess_app


def open_postprocess(
    *,
    parent: tk.Misc,
    on_open_other: Callable[[str], None] | None = None,
    on_close: Callable[[], None] | None = None,
    on_return_home: Callable[[], None] | None = None,
):
    """既存ランチャーから位況・流況後処理GUIを開く。"""
    return open_postprocess_app(
        parent=parent,
        on_open_other=on_open_other,
        on_close=on_close,
        on_return_home=on_return_home,
    )
