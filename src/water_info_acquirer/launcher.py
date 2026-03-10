"""アプリ選択ランチャー（単一Tkルート＋Toplevel子遷移）。"""

from __future__ import annotations

import tkinter as tk
from tkinter import messagebox

from .app_meta import get_app_title, get_module_title, get_version
from .app_registry import APP_DEFINITION_BY_KEY, APP_DEFINITIONS
from .runtime import ensure_src_on_path

TITLE_FONT = ("Yu Gothic UI", 20, "bold")
APPNAME_FONT = ("Yu Gothic UI", 9, "bold")
SUBTITLE_FONT = ("Yu Gothic UI", 10)
VERSION_FONT = ("Yu Gothic UI", 9)
CARD_TITLE_FONT = ("Yu Gothic UI", 12, "bold")
CARD_BODY_FONT = ("Yu Gothic UI", 9)
BUTTON_FONT = ("Yu Gothic UI", 10, "bold")

BG_ROOT = "#eef2f7"
BG_CARD = "#ffffff"
BG_CARD_ACTIVE = "#e0ecff"
BORDER_IDLE = "#d6dbe4"
BORDER_ACTIVE = "#2563eb"
TEXT_PRIMARY = "#111827"
TEXT_SECONDARY = "#4b5563"
TEXT_TERTIARY = "#6b7280"


def main() -> None:
    ensure_src_on_path()
    root = tk.Tk()
    root.title(get_app_title())
    root.geometry("640x420")
    root.minsize(560, 320)

    current_child: tk.Toplevel | None = None
    enabled_definitions = [definition for definition in APP_DEFINITIONS if definition.enabled]
    selected_key = tk.StringVar(value=enabled_definitions[0].key if enabled_definitions else "")
    card_frames: dict[str, tk.Frame] = {}

    def _on_close():
        root.destroy()

    def _return_home():
        nonlocal current_child
        if current_child:
            current_child = None
        root.deiconify()
        root.lift()
        root.focus_force()

    def _open_target(target: str | None = None) -> None:
        nonlocal current_child
        ensure_src_on_path()
        app_key = target or selected_key.get().strip()
        if not app_key:
            return
        definition = APP_DEFINITION_BY_KEY[app_key]
        if current_child:
            try:
                current_child.destroy()
            except Exception:
                pass
            current_child = None
        root.withdraw()

        def _on_open_other(next_target: str):
            _open_target(next_target)

        try:
            current_child = definition.open_app(
                parent=root,
                on_open_other=_on_open_other,
                on_close=_on_close,
                on_return_home=_return_home,
            )
        except Exception as exc:  # noqa: BLE001
            root.deiconify()
            root.lift()
            root.focus_force()
            messagebox.showerror(
                "起動エラー",
                f"{get_module_title(definition.title_key)} の起動に失敗しました。\n\n{type(exc).__name__}: {exc}",
                parent=root,
            )

    def _select_target(target: str) -> None:
        selected_key.set(target)
        _refresh_cards()

    def _refresh_cards() -> None:
        current = selected_key.get().strip()
        for key, frame in card_frames.items():
            active = key == current
            frame.configure(
                bg=BG_CARD_ACTIVE if active else BG_CARD,
                highlightbackground=BORDER_ACTIVE if active else BORDER_IDLE,
                highlightcolor=BORDER_ACTIVE if active else BORDER_IDLE,
                highlightthickness=2 if active else 1,
            )
            for child in frame.winfo_children():
                if isinstance(child, tk.Label):
                    child.configure(bg=BG_CARD_ACTIVE if active else BG_CARD)
        launch_btn.configure(state="normal" if current else "disabled")

    def _move_selection(step: int) -> None:
        if not enabled_definitions:
            return
        keys = [definition.key for definition in enabled_definitions]
        current = selected_key.get().strip()
        try:
            index = keys.index(current)
        except ValueError:
            index = 0
        selected_key.set(keys[(index + step) % len(keys)])
        _refresh_cards()

    root.configure(bg=BG_ROOT)

    outer = tk.Frame(root, bg=BG_ROOT, padx=24, pady=22)
    outer.pack(fill="both", expand=True)

    header = tk.Frame(outer, bg=BG_ROOT)
    header.pack(fill="x", pady=(0, 16))
    top_line = tk.Frame(header, bg=BG_ROOT)
    top_line.pack(fill="x")
    tk.Label(
        top_line,
        text=get_app_title(lang="en").upper(),
        bg=BG_ROOT,
        fg="#2563eb",
        font=APPNAME_FONT,
    ).pack(anchor="w", side="left")
    tk.Label(
        top_line,
        text=f"Version {get_version()}",
        bg=BG_ROOT,
        fg=TEXT_TERTIARY,
        font=VERSION_FONT,
    ).pack(anchor="e", side="right")
    tk.Label(
        header,
        text=get_app_title(),
        bg=BG_ROOT,
        fg=TEXT_PRIMARY,
        font=TITLE_FONT,
    ).pack(anchor="w", pady=(8, 0))
    tk.Label(
        header,
        text="起動する機能を選択してください",
        bg=BG_ROOT,
        fg=TEXT_SECONDARY,
        font=SUBTITLE_FONT,
    ).pack(anchor="w", pady=(6, 0))

    cards = tk.Frame(outer, bg=BG_ROOT)
    cards.pack(fill="both", expand=True)

    for definition in enabled_definitions:
        card = tk.Frame(
            cards,
            bg=BG_CARD,
            padx=14,
            pady=12,
            cursor="hand2",
            highlightbackground=BORDER_IDLE,
            highlightcolor=BORDER_IDLE,
            highlightthickness=1,
        )
        card.pack(fill="x", pady=7)
        card_frames[definition.key] = card

        title = tk.Label(
            card,
            text=get_module_title(definition.title_key),
            bg=BG_CARD,
            fg=TEXT_PRIMARY,
            font=CARD_TITLE_FONT,
            anchor="w",
            justify="left",
        )
        title.pack(anchor="w")
        body = tk.Label(
            card,
            text=definition.description or "",
            bg=BG_CARD,
            fg=TEXT_SECONDARY,
            justify="left",
            wraplength=500,
            font=CARD_BODY_FONT,
            anchor="w",
        )
        body.pack(anchor="w", pady=(6, 0), fill="x")

        for widget in (card, title, body):
            widget.bind("<Button-1>", lambda _event, key=definition.key: _select_target(key))
            widget.bind("<Double-Button-1>", lambda _event, key=definition.key: _open_target(key))

    actions = tk.Frame(outer, bg=BG_ROOT)
    actions.pack(fill="x", pady=(12, 0))
    launch_btn = tk.Button(
        actions,
        text="開く",
        command=_open_target,
        width=12,
        bg="#2563eb",
        fg="#ffffff",
        activebackground="#1d4ed8",
        activeforeground="#ffffff",
        relief="flat",
        padx=10,
        pady=6,
        cursor="hand2",
        font=BUTTON_FONT,
    )
    launch_btn.pack(anchor="e")

    footer = tk.Frame(outer, bg=BG_ROOT)
    footer.pack(fill="x", pady=(10, 0))
    tk.Label(
        footer,
        text="クリックまたは Enter で起動 / 上下キーで選択",
        bg=BG_ROOT,
        fg=TEXT_TERTIARY,
        font=VERSION_FONT,
    ).pack(anchor="w")

    root.bind("<Return>", lambda _event: _open_target())
    root.bind("<Up>", lambda _event: _move_selection(-1))
    root.bind("<Down>", lambda _event: _move_selection(1))
    _refresh_cards()
    root.update_idletasks()
    req_width = max(root.winfo_reqwidth(), 560)
    req_height = max(root.winfo_reqheight(), 320)
    screen_w = root.winfo_screenwidth()
    screen_h = root.winfo_screenheight()
    width = min(req_width, max(560, screen_w - 120))
    height = min(req_height, max(320, screen_h - 120))
    pos_x = max(0, (screen_w - width) // 2)
    pos_y = max(0, (screen_h - height) // 2)
    root.geometry(f"{width}x{height}+{pos_x}+{pos_y}")

    root.mainloop()
