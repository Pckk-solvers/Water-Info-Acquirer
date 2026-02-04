"""Dialog helpers for water_info UI."""

from __future__ import annotations

import subprocess

from tkinter import Button, Label, Toplevel


def show_error(message: str) -> None:
    win = Toplevel()
    win.title("想定外エラー")
    win.config(bg="#ff7755")
    for text in [
        "想定外のエラーが発生した可能性があります", message,
        "一度全て閉じてから再試行してください",
        "問い合わせ窓口に相談してください",
    ]:
        Label(win, text=text, bg="#ff7755").pack(padx=10, pady=5)
    Button(win, text="終了", command=win.destroy).pack(pady=10)


def show_results(parent, files: list[str], on_exit) -> None:
    parent.update_idletasks()
    x = parent.winfo_rootx()
    y = parent.winfo_rooty()

    w = Toplevel(parent)
    w.title("結果")
    w.config(bg="#d1f6ff")
    w.geometry(f"+{x}+{y}")
    w.lift()
    w.attributes("-topmost", True)
    w.after(300, lambda: w.attributes("-topmost", False))

    Label(w, text="Excel作成完了", bg="#d1f6ff").pack(pady=10)
    for f in files:
        Label(w, text=f, bg="#d1f6ff").pack()

    Button(
        w,
        text="開く",
        command=lambda: [subprocess.Popen(["start", x], shell=True) for x in files]
    ).pack(pady=5)

    Button(
        w,
        text="閉じる",
        command=w.destroy
    ).pack(pady=5)

    Button(
        w,
        text="終了",
        command=on_exit,
    ).pack(pady=5)


def show_error_popup(parent, msg: str) -> None:
    win = Toplevel(parent)
    win.title("エラー")
    win.config(bg="#ffffbf")
    px = parent.winfo_x()
    py = parent.winfo_y()
    win.geometry(f"+{px + 200}+{py + 200}")
    Label(win, text=msg, bg="#ffffbf").pack(padx=20, pady=10)
    Button(win, text="OK", command=win.destroy).pack(pady=5)
    win.transient(parent)
    win.grab_set()
    win.wait_window()
