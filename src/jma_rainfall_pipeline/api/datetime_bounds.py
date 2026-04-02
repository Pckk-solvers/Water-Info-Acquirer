from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Any


def normalize_payload_datetime(value: Any, *, field_name: str) -> Any:
    """API 入力の日時値を内部契約へ正規化する。"""

    if isinstance(value, datetime):
        normalized = value
    elif isinstance(value, date):
        normalized = datetime.combine(value, datetime.min.time())
    elif isinstance(value, str):
        text = value.strip()
        normalized = datetime.fromisoformat(text.replace("Z", "+00:00"))
    else:
        return value

    if field_name == "end_date" and normalized.time() == datetime.min.time():
        return normalized + timedelta(days=1)
    return normalized
