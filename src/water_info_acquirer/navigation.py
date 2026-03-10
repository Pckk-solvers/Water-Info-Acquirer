from __future__ import annotations

from collections.abc import Callable, Sequence
import tkinter as tk
from tkinter import messagebox
import webbrowser

from .app_meta import get_module_title
from .app_registry import APP_DEFINITION_BY_KEY, APP_DEFINITIONS


MenuAction = tuple[str, Callable[[], None]]


def build_navigation_menu(
    window: tk.Misc,
    *,
    current_app_key: str,
    on_open_other: Callable[[str], None] | None,
    on_return_home: Callable[[], None] | None = None,
    extra_actions: Sequence[MenuAction] = (),
) -> tk.Menu:
    menubar = tk.Menu(window)
    nav_menu = tk.Menu(menubar, tearoff=0)

    if on_return_home is not None:
        nav_menu.add_command(label="ランチャーへ戻る", command=on_return_home)
        nav_menu.add_separator()

    for definition in APP_DEFINITIONS:
        if not definition.enabled or definition.key == current_app_key:
            continue
        nav_menu.add_command(
            label=get_module_title(definition.title_key, lang="jp"),
            command=lambda key=definition.key: _open_other(on_open_other, key),
        )

    help_url = APP_DEFINITION_BY_KEY.get(current_app_key).help_url if current_app_key in APP_DEFINITION_BY_KEY else ""
    if help_url:
        if extra_actions:
            nav_menu.add_separator()
        nav_menu.add_command(label="ヘルプ", command=lambda: _open_help(window, help_url))

    for label, command in extra_actions:
        nav_menu.add_command(label=label, command=command)

    menubar.add_cascade(label="メニュー", menu=nav_menu)
    return menubar


def _open_other(on_open_other: Callable[[str], None] | None, app_key: str) -> None:
    if on_open_other is not None:
        on_open_other(app_key)


def _open_help(window: tk.Misc, url: str) -> None:
    try:
        opened = webbrowser.open(url)
    except Exception as exc:  # noqa: BLE001
        messagebox.showerror("ヘルプ", f"ヘルプを開けませんでした。\n\n{type(exc).__name__}: {exc}\n\n{url}", parent=window)
        return
    if not opened:
        messagebox.showinfo("ヘルプ", f"ブラウザを自動で開けませんでした。\n以下のURLを開いてください。\n\n{url}", parent=window)
