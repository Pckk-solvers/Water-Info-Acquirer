"""Debug logging helpers for water_info UI."""

from __future__ import annotations


def log(enabled: bool, *parts: object) -> None:
    if enabled:
        print(*parts)
