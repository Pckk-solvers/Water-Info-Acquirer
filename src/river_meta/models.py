from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone


MetaValue = str | list[str]
SiteMeta = dict[str, MetaValue]


def now_iso8601() -> str:
    return datetime.now(tz=timezone.utc).isoformat()


@dataclass(slots=True)
class LogEvent:
    level: str
    phase: str
    message: str
    url: str | None = None
    kind: int | None = None
    page: int | None = None
    exception_type: str | None = None
    timestamp: str = field(default_factory=now_iso8601)


@dataclass(slots=True)
class StationReport:
    station_id: str
    site_meta: SiteMeta = field(default_factory=dict)
    available_years_daily: list[int] = field(default_factory=list)
    available_years_hourly: list[int] = field(default_factory=list)
    logs: list[LogEvent] = field(default_factory=list)
