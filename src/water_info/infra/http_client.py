"""HTTP client utilities for water_info."""

from __future__ import annotations

import threading
import time
from typing import Callable, Optional

import requests
from requests import exceptions as req_exc

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/129.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;"
        "q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Language": "ja,en-US;q=0.9,en;q=0.8",
    "Connection": "close",
    # 必要なら:
    # "Referer": "http://www1.river.go.jp/",
    # "Upgrade-Insecure-Requests": "1",
}

REQUEST_MIN_DELAY = 1.0
REQUEST_DELAY_STEP = 0.2
REQUEST_MAX_DELAY = 2.0
REQUEST_MAX_RETRIES = 5
REQUEST_BACKOFF_CAP = 10
RETRYABLE_STATUS = {429, 500, 502, 503, 504}

_REQUEST_LOCK = threading.Lock()
_REQUEST_COUNTER = 0
CancelFn = Callable[[], bool]


def _calc_delay(request_index: int) -> float:
    if request_index <= 0:
        return 0.0
    delay = REQUEST_MIN_DELAY + REQUEST_DELAY_STEP * (request_index - 1)
    return min(delay, REQUEST_MAX_DELAY)


def _is_cancelled(should_stop: CancelFn | None) -> bool:
    if should_stop is None:
        return False
    try:
        return bool(should_stop())
    except Exception:
        return False


def _sleep_interruptible(seconds: float, should_stop: CancelFn | None) -> bool:
    if seconds <= 0:
        return _is_cancelled(should_stop)
    deadline = time.monotonic() + seconds
    while True:
        if _is_cancelled(should_stop):
            return True
        remain = deadline - time.monotonic()
        if remain <= 0:
            return False
        time.sleep(min(0.2, remain))


def throttled_get(url: str, headers: dict, timeout: int = 30, should_stop: CancelFn | None = None):
    """
    リクエスト間隔を最低限確保しつつ、一時的な失敗時には再試行を行うGETラッパー。
    """
    global _REQUEST_COUNTER
    last_error: Optional[Exception] = None
    for attempt in range(1, REQUEST_MAX_RETRIES + 1):
        if _is_cancelled(should_stop):
            raise req_exc.RequestException("cancelled")
        with _REQUEST_LOCK:
            current_index = _REQUEST_COUNTER
            _REQUEST_COUNTER += 1
        delay = _calc_delay(current_index)
        if delay:
            if _sleep_interruptible(delay, should_stop):
                raise req_exc.RequestException("cancelled")

        try:
            response = requests.get(url, headers=headers, timeout=timeout)
        except req_exc.RequestException as exc:
            last_error = exc
            if attempt == REQUEST_MAX_RETRIES:
                break
            backoff = min(REQUEST_MIN_DELAY * (2 ** (attempt - 1)), REQUEST_BACKOFF_CAP)
            if _sleep_interruptible(backoff, should_stop):
                raise req_exc.RequestException("cancelled")
            continue

        if response.status_code in RETRYABLE_STATUS and attempt < REQUEST_MAX_RETRIES:
            last_error = req_exc.HTTPError(
                f"HTTP {response.status_code} while requesting {url}"
            )
            backoff = min(REQUEST_MIN_DELAY * (2 ** (attempt - 1)), REQUEST_BACKOFF_CAP)
            if _sleep_interruptible(backoff, should_stop):
                raise req_exc.RequestException("cancelled")
            continue

        response.raise_for_status()
        return response

    if last_error:
        raise last_error
    raise RuntimeError(f"{url} の取得に失敗しました")
