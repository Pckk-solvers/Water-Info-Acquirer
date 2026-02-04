"""Execution wiring for water_info UI."""

from __future__ import annotations

import queue
import threading

from ..service.process_manager import ProcessManager, ProcessProgress
from .progress_window import ProgressSnapshot


class ExecutionController:
    def __init__(self, manager: ProcessManager | None = None) -> None:
        self._manager = manager or ProcessManager()

    def start(
        self,
        *,
        codes,
        request,
        fetch_hourly,
        fetch_daily,
        unit_total: int | None = None,
    ) -> queue.Queue:
        ui_queue: queue.Queue = queue.Queue()

        def _progress_adapter(progress: ProcessProgress) -> None:
            ui_queue.put(("progress", progress))

        def _error_adapter(err) -> None:
            ui_queue.put(("error", err))

        def _worker():
            print("[UI][exec] worker start")
            results = self._manager.run(
                codes=codes,
                request=request,
                fetch_hourly=fetch_hourly,
                fetch_daily=fetch_daily,
                on_progress=_progress_adapter,
                on_error=_error_adapter,
                unit_total=unit_total,
            )
            print(f"[UI][exec] worker done: results={len(results)}")
            ui_queue.put(("done", results))

        threading.Thread(target=_worker, daemon=True).start()
        return ui_queue

    @staticmethod
    def poll_queue(
        ui_queue: queue.Queue,
        *,
        on_progress,
        on_error,
        on_done,
        schedule_next,
    ) -> None:
        try:
            while True:
                kind, payload = ui_queue.get_nowait()
                if kind == "progress":
                    on_progress(payload)
                elif kind == "error":
                    on_error(payload)
                elif kind == "done":
                    on_done(payload)
                    return
        except queue.Empty:
            pass
        schedule_next()


def to_snapshot(progress: ProcessProgress, elapsed_sec: float) -> ProgressSnapshot:
    return ProgressSnapshot(
        total=progress.total,
        processed=progress.processed,
        success=progress.success,
        failed=progress.failed,
        current_code=progress.current_code,
        current_station=progress.current_station,
        unit_total=progress.unit_total,
        unit_processed=progress.unit_processed,
        elapsed_sec=elapsed_sec,
    )


def estimate_unit_total(codes, request) -> int | None:
    if not codes:
        return None
    period = request.period
    year_start = int(period.year_start)
    year_end = int(period.year_end)
    if request.options.use_daily:
        years = year_end - year_start + 1
        return max(years * len(codes), len(codes))
    month_start = int(period.month_start.replace("月", ""))
    month_end = int(period.month_end.replace("月", ""))
    total_months = (month_end - month_start + 1) + (year_end - year_start) * 12
    return max(total_months * len(codes), len(codes))
