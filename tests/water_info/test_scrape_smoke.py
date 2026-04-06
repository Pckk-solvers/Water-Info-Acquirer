import pandas as pd

from src.water_info.infra.scrape_station import extract_station_name
from src.water_info.infra.scrape_values import extract_font_values, extract_hourly_readings


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

    def get_text(self, *args, strip=False, **kwargs):
        return self._text


class _HourlyCell:
    def __init__(self, text):
        self._text = text

    def get_text(self, *args, strip=False, **kwargs):
        return self._text.strip() if strip else self._text

    def select(self, selector):
        if selector == "font":
            return [_FakeFont(self._text)]
        return []


class _HourlyRow:
    def __init__(self, cells):
        self._cells = cells

    def find_all(self, name, recursive=False):
        if name not in (["td", "th"], "td", "th"):
            return []
        return [_HourlyCell(c) for c in self._cells]


class _HourlySoup:
    def __init__(self, rows):
        self._rows = rows

    def select(self, selector):
        if selector == "tr":
            return self._rows
        if selector == "td > font":
            fonts = []
            for row in self._rows:
                for cell in row.find_all(["td", "th"], recursive=False):
                    fonts.extend(cell.select("font"))
            return fonts
        return []


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


def test_extract_hourly_readings_uses_date_row_and_24_midnight():
    soup = _HourlySoup(
        [
            _HourlyRow(
                ["2024/01/01"]
                + [str(v) for v in range(1, 25)]
            )
        ]
    )
    readings = extract_hourly_readings(soup, start_at=pd.Timestamp("2024-01-01 00:00:00").to_pydatetime())

    assert len(readings) == 24
    assert readings[0].datetime.strftime("%Y-%m-%d %H:%M:%S") == "2024-01-01 01:00:00"
    assert readings[0].value == 1.0
    assert readings[1].datetime.strftime("%Y-%m-%d %H:%M:%S") == "2024-01-01 02:00:00"
    assert readings[1].value == 2.0
    assert readings[-1].datetime.strftime("%Y-%m-%d %H:%M:%S") == "2024-01-02 00:00:00"
    assert readings[-1].value == 24.0
