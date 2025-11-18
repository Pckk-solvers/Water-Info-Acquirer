from __future__ import annotations

from typing import Protocol, Sequence


class RecordParser(Protocol):
    """HTMLから観測所情報やレコードを抽出するためのインターフェース。"""

    def parse_station(self, html: str) -> str:
        ...

    def parse_records(self, html: str, mode: str) -> Sequence:
        ...
