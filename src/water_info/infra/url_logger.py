"""URL logging utilities for water_info."""

from __future__ import annotations

from datetime import datetime


def log_urls(header: str, urls: list[str]) -> None:
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines = [f"[{timestamp}] {header}"] + urls + [""]
    print("\n".join(lines))
