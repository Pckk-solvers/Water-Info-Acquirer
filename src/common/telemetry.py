from __future__ import annotations

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable, List


@dataclass(slots=True)
class TelemetryEvent:
    """汎用的なテレメトリイベント。kindで種別を示す。"""

    kind: str
    payload: dict[str, Any]


class TelemetryService:
    """イベントを複数シンクに配信するシンプルな仕組み。"""

    def __init__(self) -> None:
        self._sinks: List[Callable[[TelemetryEvent], None]] = []

    def add_sink(self, sink: Callable[[TelemetryEvent], None]) -> None:
        self._sinks.append(sink)

    def emit(self, event: TelemetryEvent) -> None:
        for sink in list(self._sinks):
            sink(event)

    def emit_event(self, kind: str, **payload: Any) -> None:
        self.emit(TelemetryEvent(kind=kind, payload=payload))
