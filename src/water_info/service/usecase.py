"""Usecase layer for water_info."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, List

from ..domain.models import WaterInfoRequest


@dataclass
class FetchResult:
    file_path: str
    station_name: str | None = None


@dataclass
class FetchError:
    code: str
    error: Exception
    error_type: str
    message: str


@dataclass
class FetchOutcome:
    result: FetchResult | None
    error: FetchError | None


def fetch_one(
    code: str,
    request: WaterInfoRequest,
    fetch_hourly: Callable[..., str],
    fetch_daily: Callable[..., str],
    progress_callback=None,
) -> FetchResult:
    period = request.period
    options = request.options
    if options.use_daily:
        file_path = fetch_daily(
            code,
            period.year_start,
            period.year_end,
            period.month_start,
            period.month_end,
            request.mode_type,
            single_sheet=options.single_sheet,
            progress_callback=progress_callback,
        )
    else:
        file_path = fetch_hourly(
            code,
            period.year_start,
            period.year_end,
            period.month_start,
            period.month_end,
            request.mode_type,
            single_sheet=options.single_sheet,
            progress_callback=progress_callback,
        )
    station_name = _extract_station_name(file_path, code)
    return FetchResult(file_path=file_path, station_name=station_name)


def _extract_station_name(file_path: str, code: str) -> str | None:
    stem = Path(file_path).stem
    parts = stem.split("_", 2)
    if len(parts) >= 3 and parts[0] == code:
        return parts[1] or None
    return None


def fetch_for_code(
    code: str,
    request: WaterInfoRequest,
    fetch_hourly: Callable[..., str],
    fetch_daily: Callable[..., str],
    progress_callback=None,
) -> FetchOutcome:
    try:
        return FetchOutcome(
            result=fetch_one(code, request, fetch_hourly, fetch_daily, progress_callback=progress_callback),
            error=None,
        )
    except Exception as exc:  # UI層で例外種別ごとに処理する
        return FetchOutcome(
            result=None,
            error=FetchError(
                code=code,
                error=exc,
                error_type=exc.__class__.__name__,
                message=str(exc),
            ),
        )


def fetch_water_info(
    codes: Iterable[str],
    request: WaterInfoRequest,
    fetch_hourly: Callable[..., str],
    fetch_daily: Callable[..., str],
) -> tuple[List[FetchResult], List[FetchError]]:
    results: List[FetchResult] = []
    errors: List[FetchError] = []

    for code in codes:
        outcome = fetch_for_code(code, request, fetch_hourly, fetch_daily)
        if outcome.result:
            results.append(outcome.result)
        if outcome.error:
            errors.append(outcome.error)

    return results, errors
