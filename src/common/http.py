from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Callable, Dict, Optional

import requests

from src.common.telemetry import TelemetryEvent, TelemetryService


@dataclass(frozen=True)
class RetryPolicy:
    min_delay: float
    step: float
    max_delay: float
    max_retries: int
    backoff_cap: float
    retryable_status: frozenset[int] = field(default_factory=frozenset)


class ThrottledClient:
    """待機・リトライ・テレメトリを一元管理するHTTPクライアント。"""

    _lock = threading.Lock()
    _request_counter = 0

    def __init__(
        self,
        default_headers: Optional[Dict[str, str]],
        retry_policy: RetryPolicy,
        telemetry: TelemetryService,
        *,
        request_timeout: int = 30,
        request_func: Optional[Callable[[str, Dict[str, str], int], requests.Response]] = None,
        sleep_func: Optional[Callable[[float], None]] = None,
    ) -> None:
        self._headers = {**(default_headers or {})}
        self._policy = retry_policy
        self._telemetry = telemetry
        self._timeout = request_timeout
        self._request = request_func or self._default_request
        self._sleep = sleep_func or time.sleep

    def _default_request(self, url: str, headers: Dict[str, str], timeout: int) -> requests.Response:
        return requests.get(url, headers=headers, timeout=timeout)

    def _reserve_delay(self) -> float:
        with self._lock:
            index = self._request_counter
            self._request_counter += 1
        if index == 0:
            return 0.0
        delay = self._policy.min_delay + self._policy.step * (index - 1)
        return min(delay, self._policy.max_delay)

    def _emit(self, kind: str, **payload: object) -> None:
        event = TelemetryEvent(kind=kind, payload=payload)
        self._telemetry.emit(event)

    def send(self, url: str, headers: Optional[Dict[str, str]] = None) -> requests.Response:
        delay = self._reserve_delay()
        if delay:
            self._sleep(delay)

        merged_headers = {**self._headers, **(headers or {})}
        last_exc: Optional[Exception] = None
        for attempt in range(1, self._policy.max_retries + 1):
            self._emit("http.start", url=url, attempt=attempt)
            try:
                response = self._request(url, merged_headers, self._timeout)
            except requests.RequestException as exc:
                last_exc = exc
                self._handle_retry(attempt, "exception", str(exc))
                continue

            if response.status_code in self._policy.retryable_status and attempt < self._policy.max_retries:
                self._handle_retry(attempt, "status", response.status_code)
                continue

            try:
                response.raise_for_status()
            except requests.HTTPError as exc:
                last_exc = exc
                if attempt < self._policy.max_retries:
                    self._handle_retry(attempt, "error", str(exc))
                    continue
                self._emit("http.failure", url=url, error=str(exc))
                raise

            self._emit("http.success", url=url, status=response.status_code)
            return response

        self._emit("http.failure", url=url, error=str(last_exc))
        if last_exc:
            raise last_exc
        raise RuntimeError("HTTP request failed without response")

    def _handle_retry(self, attempt: int, reason: str, detail: object) -> None:
        self._emit("http.retry", reason=reason, detail=detail, attempt=attempt)
        backoff = min(self._policy.min_delay * (2 ** (attempt - 1)), self._policy.backoff_cap)
        self._sleep(backoff)
