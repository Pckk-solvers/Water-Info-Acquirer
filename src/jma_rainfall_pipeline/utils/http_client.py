"""HTTPユーティリティ: ブラウザ風ヘッダーとレート制御付きのGETを提供する。"""

from __future__ import annotations

import threading
import time
from typing import Mapping

import requests
from requests import Response, exceptions as req_exc

# ブラウザアクセスに見せるための固定ヘッダー
DEFAULT_HEADERS: dict[str, str] = {
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
}

# リクエスト制御用パラメータ
REQUEST_MIN_DELAY = 1.0
REQUEST_DELAY_STEP = 0.2
REQUEST_MAX_DELAY = 2.0
REQUEST_MAX_RETRIES = 5
REQUEST_BACKOFF_CAP = 10.0
RETRYABLE_STATUS = {429, 500, 502, 503, 504}

_REQUEST_LOCK = threading.Lock()
_REQUEST_COUNTER = 0


def _calc_delay(request_index: int) -> float:
    """リクエスト番号に応じて待機秒数を計算する。"""
    if request_index <= 0:
        return 0.0
    delay = REQUEST_MIN_DELAY + REQUEST_DELAY_STEP * (request_index - 1)
    return min(delay, REQUEST_MAX_DELAY)


def _next_request_index() -> int:
    """次のリクエスト番号をスレッドセーフに採番する。"""
    global _REQUEST_COUNTER
    with _REQUEST_LOCK:
        index = _REQUEST_COUNTER
        _REQUEST_COUNTER += 1
        return index


def throttled_get(
    url: str,
    *,
    headers: Mapping[str, str] | None = None,
    timeout: int = 30,
    rate_limit: bool = True,
) -> Response:
    """
    ブラウザ風ヘッダーを付け、必要に応じてレート制御しながらGETを実行する。

    :param url: アクセス先URL
    :param headers: 追加または上書きしたいヘッダー
    :param timeout: リクエストタイムアウト秒
    :param rate_limit: Trueなら待機を挟みFalseなら即時リクエストする
    """
    last_error: Exception | None = None
    for attempt in range(1, REQUEST_MAX_RETRIES + 1):
        if rate_limit:
            request_index = _next_request_index()
            delay = _calc_delay(request_index)
            if delay:
                time.sleep(delay)

        merged_headers = dict(DEFAULT_HEADERS)
        if headers:
            merged_headers.update(headers)

        try:
            response = requests.get(url, headers=merged_headers, timeout=timeout)
        except req_exc.RequestException as exc:
            last_error = exc
            if attempt == REQUEST_MAX_RETRIES:
                break
            backoff = min(REQUEST_MIN_DELAY * (2 ** (attempt - 1)), REQUEST_BACKOFF_CAP)
            time.sleep(backoff)
            continue

        if response.status_code in RETRYABLE_STATUS and attempt < REQUEST_MAX_RETRIES:
            last_error = req_exc.HTTPError(
                f"HTTP {response.status_code} while requesting {url}"
            )
            backoff = min(REQUEST_MIN_DELAY * (2 ** (attempt - 1)), REQUEST_BACKOFF_CAP)
            time.sleep(backoff)
            continue

        response.raise_for_status()
        return response

    if last_error:
        raise last_error
    raise RuntimeError(f"{url} の取得に失敗しました")
