"""Progress window UI for water_info."""

from __future__ import annotations

from dataclasses import dataclass
import time
from tkinter import Label, StringVar, Toplevel, ttk, TclError


@dataclass(frozen=True)
class ProgressSnapshot:
    total: int
    processed: int
    success: int
    failed: int
    current_code: str | None = None
    current_station: str | None = None
    unit_total: int | None = None
    unit_processed: int | None = None
    elapsed_sec: float = 0.0


class ProgressWindow:
    def __init__(self, parent, x: int, y: int, total_hint: int) -> None:
        self.window = Toplevel(parent)
        self.window.title("処理中")
        self.window.config(bg="#d1f6ff")
        self.window.geometry(f"+{x}+{y}")

        self._progress_var = StringVar(value="処理中... (0/0)")
        self._progress_label = Label(self.window, textvariable=self._progress_var, bg="#d1f6ff")
        self._progress_label.pack(padx=20, pady=10)

        self._progress_bar = ttk.Progressbar(self.window, orient="horizontal", length=260, mode="determinate")
        self._progress_bar.pack(padx=20, pady=5)

        self._eta_var = StringVar(value="残り時間: --:--:--")
        self._eta_label = Label(self.window, textvariable=self._eta_var, bg="#d1f6ff")
        self._eta_label.pack(padx=20, pady=10)

        if total_hint:
            self._progress_var.set(f"処理中... (0/{total_hint})")
            self._progress_bar.configure(maximum=total_hint, value=0)

        self.window.update_idletasks()

    def update(self, snapshot: ProgressSnapshot) -> bool:
        if not self.exists():
            return False
        code_label = f" 観測所 {snapshot.current_code}" if snapshot.current_code else ""
        if snapshot.current_station:
            code_label = f"{code_label} {snapshot.current_station}"

        eta_msg = "残り時間: --:--:--"
        total_for_eta = snapshot.unit_total or snapshot.total
        processed_for_eta = snapshot.unit_processed if snapshot.unit_processed is not None else snapshot.processed
        if processed_for_eta > 0:
            avg_sec = snapshot.elapsed_sec / processed_for_eta
            remaining = max(total_for_eta - processed_for_eta, 0)
            eta_sec = int(avg_sec * remaining)
            hours = eta_sec // 3600
            minutes = (eta_sec % 3600) // 60
            if hours > 0:
                eta_text = f"約{hours}時間{minutes}分"
            elif minutes > 0:
                eta_text = f"約{minutes}分"
            else:
                eta_text = "約1分未満"
            eta_msg = f"残り時間: {eta_text}"

        total = snapshot.unit_total or snapshot.total
        processed = snapshot.unit_processed if snapshot.unit_processed is not None else snapshot.processed
        try:
            self._progress_var.set(f"処理中... ({processed}/{total}){code_label}")
            self._progress_bar.configure(maximum=max(total, 1))
            self._progress_bar.configure(value=processed)
            self._eta_var.set(eta_msg)
            self.window.update_idletasks()
        except TclError:
            return False
        return True

    def destroy(self) -> None:
        self.window.update_idletasks()
        self.window.destroy()

    def exists(self) -> bool:
        return bool(self.window.winfo_exists())

    @staticmethod
    def now() -> float:
        return time.monotonic()
