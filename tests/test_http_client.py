import itertools
from dataclasses import dataclass

import pytest

from src.common.http import RetryPolicy, ThrottledClient
from src.common.telemetry import TelemetryEvent, TelemetryService


@dataclass
class DummyResponse:
    status_code: int
    text: str = "payload"

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")


def build_client(queue, responses, sleeps):
    telemetry = TelemetryService()
    telemetry.add_sink(queue.append)

    policy = RetryPolicy(
        min_delay=1.0,
        step=0.5,
        max_delay=2.0,
        max_retries=3,
        backoff_cap=5.0,
        retryable_status={429, 500, 502, 503, 504},
    )

    response_iter = iter(responses)

    def fake_request(url, headers, timeout):
        return next(response_iter)

    def fake_sleep(value):
        sleeps.append(value)

    client = ThrottledClient(
        default_headers={"User-Agent": "test"},
        retry_policy=policy,
        telemetry=telemetry,
        request_func=fake_request,
        sleep_func=fake_sleep,
    )
    return client, telemetry


def test_send_success_without_retry(monkeypatch):
    queue, sleeps = [], []
    client, telemetry = build_client(queue, [DummyResponse(200)], sleeps)

    resp = client.send("http://example.com")

    assert resp.status_code == 200
    assert [e.kind for e in queue] == ["http.start", "http.success"]
    assert sleeps == []


def test_send_retries_and_emits_events(monkeypatch):
    queue, sleeps = [], []
    responses = [DummyResponse(429), DummyResponse(200)]
    client, telemetry = build_client(queue, responses, sleeps)

    resp = client.send("http://example.com")

    assert resp.status_code == 200
    assert [e.kind for e in queue] == [
        "http.start",
        "http.retry",
        "http.start",
        "http.success",
    ]
    # 第2回目のリクエスト前にインターバル待機、リトライ時にバックオフが入る
    assert len(sleeps) == 1
    assert sleeps[0] == pytest.approx(1.0)


def test_request_index_increments_between_calls():
    queue, sleeps = [], []
    responses = [DummyResponse(200), DummyResponse(200)]
    client, telemetry = build_client(queue, responses, sleeps)

    client.send("http://example.com")
    client.send("http://example.com/next")

    # 2回目の呼び出し前にインターバル待機が発生する
    assert sleeps == [pytest.approx(1.0)]
