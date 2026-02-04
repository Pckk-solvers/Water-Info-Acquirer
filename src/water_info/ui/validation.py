"""Input validation helpers for water_info UI."""

from __future__ import annotations

import time
from dataclasses import dataclass

from ..domain.models import Options, Period, WaterInfoRequest


def format_input_error_message(exc: Exception) -> str:
    return f"入力エラー: {exc}"


@dataclass
class ValidationState:
    last_message: str | None = None
    last_at: float = 0.0


class InputValidator:
    def __init__(self, cooldown_sec: float = 1.0) -> None:
        self._cooldown_sec = cooldown_sec
        self._state = ValidationState()

    def can_validate(self, year_start: str, year_end: str, month_start: str, month_end: str) -> bool:
        if not year_start or not year_end:
            return False
        if len(year_start) != 4 or len(year_end) != 4:
            return False
        if not month_start or not month_end:
            return False
        return True

    def build_request(
        self,
        *,
        year_start: str,
        year_end: str,
        month_start: str,
        month_end: str,
        mode_type: str,
        use_daily: bool,
        single_sheet: bool,
    ) -> WaterInfoRequest:
        return WaterInfoRequest(
            period=Period(
                year_start=year_start,
                year_end=year_end,
                month_start=month_start,
                month_end=month_end,
            ),
            mode_type=mode_type,
            options=Options(
                use_daily=use_daily,
                single_sheet=single_sheet,
            ),
        )

    def should_throttle(self, message: str) -> bool:
        now = time.monotonic()
        if self._state.last_message == message and (now - self._state.last_at) < self._cooldown_sec:
            return True
        self._state.last_message = message
        self._state.last_at = now
        return False
