from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Iterable, List, Sequence

from src.common.http import ThrottledClient
from src.core.parser import RecordParser


MODE_CONFIGS = {
    "S": {"kind": "2", "mode_str": "Water"},
    "R": {"kind": "6", "mode_str": "Water"},
    "U": {"kind": "2", "mode_str": "Rain"},
}


@dataclass(frozen=True)
class FetchRequest:
    code: str
    mode: str
    period_start: date
    period_end: date
    granularity: str = "hourly"


@dataclass(frozen=True)
class FetchResponse:
    code: str
    station_name: str
    records: Sequence
    coverage: tuple[date | None, date | None]
    mode: str


class FetchService:
    def __init__(self, client: ThrottledClient, parser: RecordParser) -> None:
        self._client = client
        self._parser = parser

    def fetch(self, request: FetchRequest) -> FetchResponse:
        config = MODE_CONFIGS.get(request.mode)
        if not config:
            raise ValueError(f"Unsupported mode: {request.mode}")
        month_keys = list(self._month_keys(request.period_start, request.period_end))
        if not month_keys:
            raise ValueError("Invalid period range")

        first_url = self._build_url(request, config, month_keys[0])
        first_resp = self._client.send(first_url)
        station = self._parser.parse_station(first_resp.text)
        records: List = []
        records.extend(self._parser.parse_records(first_resp.text, request.mode))

        for key in month_keys[1:]:
            url = self._build_url(request, config, key)
            resp = self._client.send(url)
            records.extend(self._parser.parse_records(resp.text, request.mode))

        return FetchResponse(
            code=request.code,
            station_name=station,
            records=records,
            coverage=(request.period_start, request.period_end),
            mode=request.mode,
        )

    def _build_url(self, request: FetchRequest, config: dict, month_key: str) -> str:
        base = f"http://www1.river.go.jp/cgi-bin/Dsp{config['mode_str']}Data.exe"
        end_year = request.period_end.year
        return (
            f"{base}?KIND={config['kind']}&ID={request.code}&BGNDATE={month_key}"
            f"&ENDDATE={end_year}1231&KAWABOU=NO"
        )

    def _month_keys(self, start: date, end: date) -> Iterable[str]:
        current = date(start.year, start.month, 1)
        limit = date(end.year, end.month, 1)
        while current <= limit:
            yield current.strftime("%Y%m01")
            # advance to next month
            next_month = current.replace(day=28) + timedelta(days=4)
            current = next_month.replace(day=1)
