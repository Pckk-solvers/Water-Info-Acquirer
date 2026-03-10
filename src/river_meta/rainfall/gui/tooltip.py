from __future__ import annotations

import tkinter as tk
from collections.abc import Callable
from typing import ClassVar


class ToolTip:
    """Simple tooltip for Tk widgets."""
    _active: ClassVar["ToolTip | None"] = None

    def __init__(
        self,
        widget: tk.Widget,
        text: str | None = None,
        *,
        text_fn: Callable[[], str] | None = None,
        delay_ms: int = 500,
        wraplength: int = 320,
    ) -> None:
        self.widget = widget
        self._text = text or ""
        self._text_fn = text_fn
        self._delay_ms = max(0, int(delay_ms))
        self._wraplength = max(120, int(wraplength))
        self._after_id: str | None = None
        self._tip_window: tk.Toplevel | None = None
        self._label: tk.Label | None = None

        self.widget.bind("<Enter>", self._on_enter, add="+")
        self.widget.bind("<Leave>", self._on_leave, add="+")
        self.widget.bind("<ButtonPress>", self._on_leave, add="+")
        self.widget.bind("<Destroy>", self._on_leave, add="+")

    def set_text(self, text: str) -> None:
        self._text = text
        if self._label is not None:
            self._label.configure(text=self._resolve_text())

    def _resolve_text(self) -> str:
        if self._text_fn is not None:
            try:
                return str(self._text_fn() or "")
            except Exception:
                return self._text
        return self._text

    def _on_enter(self, _event: tk.Event) -> None:  # type: ignore[type-arg]
        self._schedule_show()

    def _on_leave(self, _event: tk.Event) -> None:  # type: ignore[type-arg]
        self._cancel_show()
        self._hide()

    def _schedule_show(self) -> None:
        self._cancel_show()
        try:
            self._after_id = self.widget.after(self._delay_ms, self._show)
        except tk.TclError:
            self._after_id = None

    def _cancel_show(self) -> None:
        if self._after_id is None:
            return
        try:
            self.widget.after_cancel(self._after_id)
        except tk.TclError:
            pass
        self._after_id = None

    def _show(self) -> None:
        self._after_id = None
        if self._tip_window is not None:
            return

        # 常に1つだけ表示する
        if ToolTip._active is not None and ToolTip._active is not self:
            ToolTip._active._cancel_show()
            ToolTip._active._hide()

        text = self._resolve_text().strip()
        if not text:
            return

        try:
            x = self.widget.winfo_pointerx() + 14
            y = self.widget.winfo_pointery() + 16
        except tk.TclError:
            return

        tip = tk.Toplevel(self.widget)
        tip.wm_overrideredirect(True)
        tip.wm_geometry(f"+{x}+{y}")
        label = tk.Label(
            tip,
            text=text,
            justify="left",
            background="#fffde9",
            foreground="#1f2937",
            relief="solid",
            borderwidth=1,
            padx=6,
            pady=4,
            wraplength=self._wraplength,
        )
        label.pack()
        self._tip_window = tip
        self._label = label
        ToolTip._active = self

    def _hide(self) -> None:
        if self._tip_window is None:
            return
        try:
            self._tip_window.destroy()
        except tk.TclError:
            pass
        self._tip_window = None
        self._label = None
        if ToolTip._active is self:
            ToolTip._active = None
