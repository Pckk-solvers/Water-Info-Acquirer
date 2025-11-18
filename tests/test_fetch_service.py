from datetime import date
from dataclasses import dataclass

import pytest

from src.common.http import ThrottledClient, RetryPolicy
from src.common.telemetry import TelemetryService
from src.core.fetch import FetchRequest, FetchResponse, FetchService


@dataclass
class DummyResponse:
    text: str
    status_code: int = 200

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise Exception(f"HTTP {self.status_code}")


class StubClient(ThrottledClient):
    def __init__(self, responses):
        self.responses = iter(responses)
        self.calls = []
        telemetry = TelemetryService()
        telemetry.add_sink(lambda e: None)
        policy = RetryPolicy(1, 0, 1, 1, 1)
        super().__init__(default_headers=None, retry_policy=policy, telemetry=telemetry,
                         request_func=self._request, sleep_func=lambda x: None)

    def _request(self, url, headers, timeout):
        self.calls.append(url)
        try:
            return next(self.responses)
        except StopIteration:
            raise AssertionError("No more responses")


def build_service(responses, parser):
    client = StubClient(responses)
    service = FetchService(client=client, parser=parser)
    return service, client


class DummyParser:
    def __init__(self, station_name="Station", record_map=None):
        self.station_name = station_name
        self.record_map = record_map or {}
        self.station_calls = []
        self.record_calls = []

    def parse_station(self, html):
        self.station_calls.append(html)
        return self.station_name

    def parse_records(self, html, mode):
        self.record_calls.append((html, mode))
        return self.record_map.get(html, [])


def _request():
    return FetchRequest(
        code="123456",
        mode="S",
        period_start=date(2024, 1, 1),
        period_end=date(2024, 2, 1),
        granularity="hourly",
    )


def test_fetch_builds_urls_and_uses_parser():
    parser = DummyParser(record_map={"meta": [1, 2], "feb": [3]})
    responses = [DummyResponse("meta"), DummyResponse("feb")]
    service, client = build_service(responses, parser)

    response = service.fetch(_request())

    assert isinstance(response, FetchResponse)
    assert response.records == [1, 2, 3]
    assert parser.station_calls == ["meta"]
    assert parser.record_calls[0][0] == "meta"
    assert parser.record_calls[1][0] == "feb"
    expected_urls = [
        "http://www1.river.go.jp/cgi-bin/DspWaterData.exe?KIND=2&ID=123456&BGNDATE=20240101&ENDDATE=20241231&KAWABOU=NO",
        "http://www1.river.go.jp/cgi-bin/DspWaterData.exe?KIND=2&ID=123456&BGNDATE=20240201&ENDDATE=20241231&KAWABOU=NO",
    ]
    assert client.calls == expected_urls


def test_fetch_raises_for_unknown_mode():
    parser = DummyParser()
    service, _ = build_service([DummyResponse("meta")], parser)
    bad_request = FetchRequest(code="123", mode="X", period_start=date(2024, 1, 1), period_end=date(2024, 1, 1))
    with pytest.raises(ValueError):
        service.fetch(bad_request)


