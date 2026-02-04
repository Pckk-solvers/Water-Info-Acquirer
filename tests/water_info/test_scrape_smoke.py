from src.water_info.infra.scrape_station import extract_station_name
from src.water_info.infra.scrape_values import extract_font_values


class _FakeTd:
    def __init__(self, text):
        self._text = text

    def get_text(self, strip=False):
        return self._text.strip() if strip else self._text


class _FakeTr:
    def __init__(self, cells):
        self._cells = cells

    def find_all(self, name):
        if name != "td":
            return []
        return [_FakeTd(c) for c in self._cells]


class _FakeTable:
    def __init__(self, station_name):
        self._station_name = station_name

    def find_all(self, name):
        if name != "tr":
            return []
        return [_FakeTr(["h1", "h2"]), _FakeTr(["code", self._station_name])]


class _FakeFont:
    def __init__(self, text):
        self._text = text

    def get_text(self):
        return self._text


class _FakeSoup:
    def __init__(self, station_name=None, values=None):
        self._station_name = station_name
        self._values = values or []

    def find_all(self, name, attrs=None):
        if name == "table":
            return [_FakeTable(self._station_name)]
        return []

    def select(self, selector):
        if selector == "td > font":
            return [_FakeFont(v) for v in self._values]
        return []


def test_extract_station_name_strips_kana():
    soup = _FakeSoup(station_name="神野瀬川（かんのせがわ）")
    assert extract_station_name(soup) == "神野瀬川"


def test_extract_font_values():
    soup = _FakeSoup(values=["1", "2", "3"])
    assert extract_font_values(soup) == ["1", "2", "3"]
