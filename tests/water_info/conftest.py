import pandas as pd
import pytest

from src.water_info import main_datetime
from src.water_info import datemode
from src.water_info.infra import http_client


class FakeResponse:
    def __init__(self, payload):
        self.text = payload
        self.status_code = 200
        self.encoding = None

    def raise_for_status(self):
        return None


class FakeFont:
    def __init__(self, value):
        self.value = value

    def __iter__(self):
        return iter([self.value])

    def get_text(self):
        return self.value


class FakeTd:
    def __init__(self, text):
        self.text = text

    def get_text(self, strip=False):
        return self.text.strip() if strip else self.text


class FakeTr:
    def __init__(self, cells):
        self._cells = cells

    def find_all(self, name):
        if name != "td":
            return []
        return [FakeTd(cell) for cell in self._cells]


class FakeTable:
    def __init__(self, station_name):
        self.station_name = station_name

    def find_all(self, name):
        if name != "tr":
            return []
        header = FakeTr(["h1", "h2"])
        data = FakeTr(["code", self.station_name])
        return [header, data]


class FakeSoup:
    def __init__(self, payload, parser=None):
        self.payload = payload if isinstance(payload, dict) else {}

    def find_all(self, name, attrs=None):
        if name == "table":
            station = self.payload.get("station", "テスト観測所（かな）")
            return [FakeTable(station)]
        return []

    def select(self, selector):
        if selector == "td > font":
            values = self.payload.get("values", [])
            return [FakeFont(v) for v in values]
        return []


@pytest.fixture(autouse=True)
def _reset_request_counter(monkeypatch):
    monkeypatch.setattr(http_client, "_REQUEST_COUNTER", 0, raising=False)
    monkeypatch.setattr(http_client, "_REQUEST_LOCK", http_client.threading.Lock(), raising=False)


@pytest.fixture()
def fake_bs4(monkeypatch):
    monkeypatch.setattr(main_datetime, "BeautifulSoup", FakeSoup)
    monkeypatch.setattr(datemode, "BeautifulSoup", FakeSoup)
    return FakeSoup


@pytest.fixture()
def fake_station_payload():
    return {"station": "テスト観測所（かな）"}


@pytest.fixture()
def make_values_payload():
    def _factory(values):
        return {"values": values}
    return _factory


@pytest.fixture()
def fake_throttled_get_factory():
    def _factory(payloads):
        calls = {"count": 0}

        def _stub(url, headers=None, timeout=30):
            idx = calls["count"]
            calls["count"] += 1
            payload = payloads[min(idx, len(payloads) - 1)]
            return FakeResponse(payload)

        return _stub

    return _factory
