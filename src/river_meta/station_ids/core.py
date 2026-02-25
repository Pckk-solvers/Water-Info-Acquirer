from __future__ import annotations

import time
from typing import Callable, Optional

import requests

from .extractors import (
    extract_komoku_options,
    extract_prefectures,
    extract_station_ids,
    extract_total_count,
)


SRCHSITE_URL = "https://www1.river.go.jp/cgi-bin/SrchSite.exe"
DEFAULT_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/122.0.0.0 Safari/537.36"
)
MASTER_PARAMS = {
    "CITY": "",
    "KASEN": "",
    "KEN": "-1",
    "KOMOKU": "-1",
    "NAME": "",
    "SUIKEI": "-00001",
}


def decode_html(content: bytes) -> str:
    for encoding in ("euc_jp", "shift_jis", "cp932", "utf-8"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    return content.decode("utf-8", errors="replace")


def detect_access_restriction(html: str) -> bool:
    lowered = html.lower()
    return "access restrictions" in lowered and "prohibits data acquisition" in lowered


def build_session(user_agent: str = DEFAULT_UA) -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
        }
    )
    return session


def fetch_page(session: requests.Session, page: int, params: dict, timeout: float) -> str:
    payload = dict(params)
    payload["PAGE"] = str(page)
    response = session.get(SRCHSITE_URL, params=payload, timeout=timeout)
    response.raise_for_status()
    html = decode_html(response.content)
    if detect_access_restriction(html):
        raise PermissionError("Access Restrictions: server denied automated access.")
    return html


def fetch_master_options(
    session: requests.Session,
    *,
    timeout: float,
) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    html = fetch_page(session, page=0, params=MASTER_PARAMS, timeout=timeout)
    return extract_prefectures(html), extract_komoku_options(html)


def collect_station_ids(
    session: requests.Session,
    *,
    params: dict,
    timeout: float,
    sleep_sec: float,
    page_max: int,
    info_log: Callable[[str], None] | None = None,
    warn_log: Callable[[str], None] | None = None,
) -> tuple[list[str], Optional[int]]:
    ids: set[str] = set()

    html0 = fetch_page(session, page=0, params=params, timeout=timeout)
    total = extract_total_count(html0)
    ids |= extract_station_ids(html0)

    if total is not None:
        est_last_page = (max(total - 1, 0)) // 10
        page_end = min(page_max, est_last_page + 5)
    else:
        page_end = page_max

    empty_streak = 0
    for page in range(1, page_end + 1):
        time.sleep(sleep_sec)
        try:
            html = fetch_page(session, page=page, params=params, timeout=timeout)
        except Exception as exc:  # noqa: BLE001
            if warn_log:
                warn_log(f"[WARN] fetch failed page={page}: {exc}")
            continue

        found = extract_station_ids(html)
        if not found:
            empty_streak += 1
            if empty_streak >= 3:
                break
            continue

        empty_streak = 0
        ids |= found
        if info_log and page % 50 == 0:
            info_log(f"[INFO] page={page} ids={len(ids)}")

    def sort_key(value: str) -> tuple[int, int | str]:
        try:
            return (0, int(value))
        except ValueError:
            return (1, value)

    return sorted(ids, key=sort_key), total
