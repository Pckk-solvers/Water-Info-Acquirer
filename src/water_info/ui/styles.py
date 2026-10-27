"""取得画面だけに適用するttkスタイル。"""

from tkinter import ttk

BACKGROUND = "#eef3f7"
SURFACE = "#ffffff"
INK = "#183142"


def configure_styles(master):
    style = ttk.Style(master)
    font = ("Yu Gothic UI", 10)
    style.configure("Water.TFrame", background=BACKGROUND)
    style.configure("Water.Card.TFrame", background=SURFACE)
    style.configure("Water.TLabel", background=BACKGROUND, foreground=INK, font=font)
    style.configure("Water.Title.TLabel", background=BACKGROUND, foreground=INK, font=("Yu Gothic UI", 19, "bold"))
    style.configure("Water.Card.TLabel", background=SURFACE, foreground=INK, font=font)
    style.configure("Water.Section.TLabel", background=SURFACE, foreground=INK, font=("Yu Gothic UI", 11, "bold"))
    style.configure("Water.Hint.TLabel", background=SURFACE, foreground="#526978", font=("Yu Gothic UI", 9))
    style.configure("Water.Error.TLabel", background=BACKGROUND, foreground="#b42318", font=font)
    style.configure("Water.TButton", padding=(10, 5), font=font)
    style.configure("Water.Primary.TButton", padding=(18, 7), font=("Yu Gothic UI", 11, "bold"), foreground="#006b70")
    for widget in ("TRadiobutton", "TCheckbutton"):
        style.configure("Water." + widget, background=SURFACE, foreground=INK, font=font)
    style.configure("Water.Treeview", rowheight=29, font=font)
    style.configure("Water.Treeview.Heading", font=("Yu Gothic UI", 10, "bold"))


def card(parent, title):
    frame = ttk.Frame(parent, padding=14, style="Water.Card.TFrame")
    ttk.Label(frame, text=title, style="Water.Section.TLabel").pack(anchor="w", pady=(0, 10))
    return frame
