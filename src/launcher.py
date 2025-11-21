"""アプリ選択ランチャー（単一Tkルート＋Toplevel子遷移）。"""

import sys
import os
import tkinter as tk
import threading
import importlib
from pathlib import Path
from tkinter import messagebox

from src.app_names import get_app_title, get_module_title, get_version

IS_FROZEN = getattr(sys, "frozen", False)
PROJECT_ROOT = Path(sys.executable).resolve().parent if IS_FROZEN else Path(__file__).resolve().parent.parent
SRC_DIR = PROJECT_ROOT / "src"


def _ensure_src_on_path() -> None:
    """凍結/非凍結どちらでも water_info / jma_rainfall_pipeline を解決できるようにする。"""
    try:
        os.chdir(PROJECT_ROOT)
    except OSError:
        pass
    src_path = str(SRC_DIR)
    if src_path not in sys.path:
        sys.path.insert(0, src_path)


def _preload_dependencies(status_label: tk.Label, buttons: list[tk.Button]) -> None:
    """裏で依存を読み込んでボタンを有効化する。"""
    _ensure_src_on_path()
    errors: list[str] = []
    try:
        importlib.import_module("water_info.main_datetime")
    except Exception as exc:
        errors.append(f"water_info のロードに失敗: {exc}")
    try:
        importlib.import_module("jma_rainfall_pipeline.main")
        importlib.import_module("jma_rainfall_pipeline.gui.app")
    except Exception as exc:
        errors.append(f"jma_rainfall_pipeline のロードに失敗: {exc}")

    def _finish() -> None:
        if errors:
            status_label.config(text="依存読み込みに失敗しました")
            messagebox.showerror("依存チェックエラー", "\n".join(errors))
        else:
            status_label.config(text="準備完了")
            for btn in buttons:
                btn.config(state=tk.NORMAL)

    status_label.after(0, _finish)


def main() -> None:
    _ensure_src_on_path()
    root = tk.Tk()
    root.title(f"{get_app_title()} v{get_version()}")  # 共通タイトルにバージョン表記
    root.geometry("320x180")

    current_child: tk.Toplevel | None = None

    def _on_close():
        root.destroy()

    def _open_target(target: str) -> None:
        nonlocal current_child
        _ensure_src_on_path()
        if current_child:
            try:
                current_child.destroy()
            except Exception:
                pass
            current_child = None
        root.withdraw()

        def _on_open_other(next_target: str):
            _open_target(next_target)

        if target == "water":
            from water_info.main_datetime import show_water

            current_child = show_water(parent=root, on_open_other=_on_open_other, on_close=_on_close)
        else:
            from jma_rainfall_pipeline.gui.app import show_jma

            current_child = show_jma(parent=root, on_open_other=_on_open_other, on_close=_on_close)

    tk.Label(root, text="起動するアプリを選んでください").pack(pady=8)
    status_label = tk.Label(root, text="依存を読み込み中です…")
    status_label.pack(pady=4)

    buttons: list[tk.Button] = []
    tk.Button(
        root,
        text=f"{get_module_title('water_info')} ",
        state=tk.DISABLED,
        command=lambda: _open_target("water"),
    ).pack(pady=6)

    tk.Button(
        root,
        text=get_module_title('jma'),
        state=tk.DISABLED,
        command=lambda: _open_target("jma"),
    ).pack(pady=6)

    for child in root.winfo_children():
        if isinstance(child, tk.Button):
            buttons.append(child)

    threading.Thread(
        target=_preload_dependencies,
        args=(status_label, buttons),
        daemon=True,
    ).start()

    root.mainloop()


if __name__ == "__main__":
    main()
