from __future__ import annotations

import time
from pathlib import Path

import requests

from .fetcher import FetchResult, fetch_html
from .models import LogEvent, StationReport
from .parser_availability import parse_availability_page
from .parser_siteinfo import parse_siteinfo


SITE_INFO_URL = "https://www1.river.go.jp/cgi-bin/SiteInfo.exe?ID={station_id}"
RAIN_URL = "https://www1.river.go.jp/cgi-bin/SrchRainData.exe?ID={station_id}&KIND={kind}&PAGE={page}"
DEFAULT_USER_AGENT = "river-meta/0.1"


def _append_log(
    report: StationReport,
    *,
    level: str,
    phase: str,
    message: str,
    url: str | None = None,
    kind: int | None = None,
    page: int | None = None,
    exception_type: str | None = None,
) -> None:
    report.logs.append(
        LogEvent(
            level=level,
            phase=phase,
            message=message,
            url=url,
            kind=kind,
            page=page,
            exception_type=exception_type,
        )
    )


def _save_debug_html(
    html: str,
    *,
    station_id: str,
    suffix: str,
    debug_save_html_dir: str | None,
) -> None:
    if not debug_save_html_dir:
        return
    output_dir = Path(debug_save_html_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / f"{station_id}_{suffix}.html"
    path.write_text(html, encoding="utf-8")


def _handle_fetch_failure(
    report: StationReport,
    *,
    result: FetchResult,
    phase: str,
    kind: int | None = None,
    page: int | None = None,
) -> None:
    detail = result.error or "Unknown error"
    if result.status_code is not None:
        detail = f"{detail} (status={result.status_code})"
    _append_log(
        report,
        level="ERROR",
        phase=phase,
        message=detail,
        url=result.url,
        kind=kind,
        page=page,
        exception_type=result.exception_type,
    )


def scrape_station(
    id: str,
    *,
    kinds: tuple[int, ...] | list[int] = (2, 3),
    page_scan_max: int = 20,
    timeout: float = 10,
    user_agent: str | None = None,
    request_interval_ms: int = 0,
    debug_save_html_dir: str | None = None,
) -> StationReport:
    report = StationReport(station_id=id)
    session = requests.Session()
    ua = user_agent or DEFAULT_USER_AGENT
    sleep_sec = max(0, request_interval_ms) / 1000

    site_info_url = SITE_INFO_URL.format(station_id=id)
    site_result = fetch_html(session, site_info_url, timeout_sec=timeout, user_agent=ua)
    if site_result.ok and site_result.html:
        _save_debug_html(
            site_result.html,
            station_id=id,
            suffix="siteinfo",
            debug_save_html_dir=debug_save_html_dir,
        )
        report.site_meta = parse_siteinfo(site_result.html)
        _append_log(
            report,
            level="INFO",
            phase="parse",
            message=f"Parsed SiteInfo keys: {len(report.site_meta)}",
            url=site_result.url,
        )
    else:
        _handle_fetch_failure(report, result=site_result, phase="fetch")

    for kind in kinds:
        if kind not in (2, 3):
            _append_log(
                report,
                level="WARN",
                phase="config",
                message=f"Unsupported kind skipped: {kind}",
                kind=kind,
            )
            continue

        all_years: list[int] = []
        for page in range(page_scan_max + 1):
            page_url = RAIN_URL.format(station_id=id, kind=kind, page=page)
            result = fetch_html(session, page_url, timeout_sec=timeout, user_agent=ua)
            if not result.ok or not result.html:
                _handle_fetch_failure(
                    report,
                    result=result,
                    phase="fetch",
                    kind=kind,
                    page=page,
                )
                break

            _save_debug_html(
                result.html,
                station_id=id,
                suffix=f"kind{kind}_page{page}",
                debug_save_html_dir=debug_save_html_dir,
            )
            page_years, has_decade_table = parse_availability_page(result.html)
            if not has_decade_table:
                if page == 0:
                    _append_log(
                        report,
                        level="WARN",
                        phase="parse",
                        message="No availability decade table found.",
                        url=page_url,
                        kind=kind,
                        page=page,
                    )
                break

            all_years.extend(page_years)
            if sleep_sec > 0:
                time.sleep(sleep_sec)

        normalized_years = sorted(set(all_years))
        if kind == 3:
            report.available_years_daily = normalized_years
        elif kind == 2:
            report.available_years_hourly = normalized_years

        _append_log(
            report,
            level="INFO",
            phase="parse",
            message=f"KIND={kind} available years: {len(normalized_years)}",
            kind=kind,
        )

    return report
