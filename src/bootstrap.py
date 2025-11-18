from __future__ import annotations

from typing import Optional

from src.core.app import AppService


def build_app_service() -> Optional[AppService]:
    """Return a configured AppService or None to fallback to legacy GUI."""
    return None
