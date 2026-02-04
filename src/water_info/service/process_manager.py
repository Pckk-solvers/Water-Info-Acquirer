"""Process management for water_info executions."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable, List, Optional

from .usecase import fetch_for_code


@dataclass(frozen=True)
class ProcessProgress:
    total: int
    processed: int
    success: int
    failed: int
    current_code: Optional[str] = None
    current_station: Optional[str] = None
    unit_total: Optional[int] = None
    unit_processed: Optional[int] = None


class ProcessManager:
    def run(
        self,
        codes: Iterable[str],
        request,
        fetch_hourly,
        fetch_daily,
        *,
        on_progress: Optional[Callable[[ProcessProgress], None]] = None,
        on_error: Optional[Callable[[object], None]] = None,
        unit_total: Optional[int] = None,
    ) -> List[object]:
        code_list = list(codes)
        total = len(code_list)
        processed = 0
        success = 0
        failed = 0
        results: List[object] = []
        unit_processed = 0
        current_station = None

        if on_progress:
            on_progress(
                ProcessProgress(
                    total=total,
                    processed=0,
                    success=0,
                    failed=0,
                    unit_total=unit_total,
                    unit_processed=unit_processed,
                )
            )

        for code in code_list:
            def _on_unit(*, increment: bool = True, station_name: Optional[str] = None):
                nonlocal unit_processed, current_station
                if station_name:
                    current_station = station_name
                if increment:
                    unit_processed += 1
                if on_progress:
                    on_progress(
                        ProcessProgress(
                            total=total,
                            processed=processed,
                            success=success,
                            failed=failed,
                            current_code=code,
                            current_station=current_station,
                            unit_total=unit_total,
                            unit_processed=unit_processed,
                        )
                    )

            outcome = fetch_for_code(
                code=code,
                request=request,
                fetch_hourly=fetch_hourly,
                fetch_daily=fetch_daily,
                progress_callback=_on_unit,
            )
            if outcome.result:
                results.append(outcome.result)
                success += 1
                station_name = outcome.result.station_name
            else:
                station_name = None
            if outcome.error:
                failed += 1
                if on_error:
                    on_error(outcome.error)

            processed += 1
            if on_progress:
                on_progress(
                    ProcessProgress(
                        total=total,
                        processed=processed,
                        success=success,
                        failed=failed,
                        current_code=code,
                        current_station=station_name or current_station,
                        unit_total=unit_total,
                        unit_processed=unit_processed,
                    )
                )

        return results
